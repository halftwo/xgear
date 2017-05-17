#include "RedisClient.h"
#include "dlog/dlog.h"
#include "xslib/XEvent.h"
#include "xslib/xnet.h"
#include "xslib/loc.h"
#include "xslib/iobuf.h"
#include "xslib/xlog.h"
#include "xslib/vbs.h"
#include <assert.h>
#include <errno.h>
#include <unistd.h>

#define REDIS_PORT	6379
#define DEFAULT_CON_NUM	6

#define MAX_LEVEL	10

#define GIVEUP_TIMEOUT		(2*1000)
#define SHUTDOWN_TIMEOUT	(5*1000)
#define RETRY_INTERVAL		(15*1000)

class RConnection: public XEvent::FdHandler, public XEvent::TaskHandler, private XMutex
{
public:
	RConnection(RedisClient* mclient);
	virtual ~RConnection();

	void process(const RedisOperationPtr& op);
	void shutdown();
	void setIdle(bool idle);
	bool ok() const				{ return (_state == ST_WAIT); }
	bool closed() const			{ return (_state == ST_CLOSED); }
	void reconnect();
	void game_over(const XEvent::DispatcherPtr& dispatcher);

	virtual void event_on_fd(const XEvent::DispatcherPtr& dispatcher, int events);
	virtual void event_on_task(const XEvent::DispatcherPtr& dispatcher);

private:
	struct ctx_t {
		loc_t loc;
		vbs_data_t data;
		size_t item_num;
		size_t item_cur;
	};
	int read_one_item(int level);
	int do_read(const XEvent::DispatcherPtr& dispatcher);
	int do_write(const XEvent::DispatcherPtr& dispatcher);
	void do_close(const XEvent::DispatcherPtr& dispatcher, bool retry);

private:
	RedisClientPtr _client;
	RedisOperationPtr _op;
	int _fd;

	bool _shutdown;
	bool _idle;	// if true, it's in RedisClient::_istack

	enum {
		ST_CONNECT,
		ST_WAIT,
		ST_WRITE,
		ST_READ,
		ST_CLOSED,
	} _state;

        iobuf_t _ib;
        unsigned char _ibuf[1024];
        loc_t _iloc;
	size_t _icmd;
	ctx_t _ctx[MAX_LEVEL];
	xstr_t _chunk;
	ssize_t _chunk_pos;

        loc_t _oloc;
        struct iovec *_ov;
        int _ov_num;
};

class ShutdownTimeoutHandler: public XEvent::TaskHandler
{
	RConnectionPtr _con;
public:
	ShutdownTimeoutHandler(RConnection* con)
		: _con(con)
	{
	}

	virtual void event_on_task(const XEvent::DispatcherPtr& dispatcher)
	{
		_con->game_over(dispatcher);
	}
};


RConnection::RConnection(RedisClient* mclient)
	: _client(mclient)
{
	_shutdown = false;
	_idle = false;
	_fd = -1;
}

RConnection::~RConnection()
{
	if (_fd >= 0)
		::close(_fd);
}

void RConnection::process(const RedisOperationPtr& op)
{
	Lock lock(*this);
	assert(!_op && op);
	_op = op;
	_state = ST_WRITE;

	XEvent::DispatcherPtr dispatcher = _client->dispatcher();
	if (do_write(dispatcher) < 0)
		do_close(dispatcher, true);
	else
		dispatcher->replaceTask(this, GIVEUP_TIMEOUT);
}

void RConnection::shutdown()
{
	Lock lock(*this);
	_shutdown = true;
	_client->dispatcher()->replaceTask(new ShutdownTimeoutHandler(this), SHUTDOWN_TIMEOUT);
}

void RConnection::game_over(const XEvent::DispatcherPtr& dispatcher)
{
	Lock lock(*this);
	do_close(dispatcher, false);
}

void RConnection::setIdle(bool idle)
{
	_idle = idle;
}

void RConnection::reconnect()
{
	if (_fd >= 0)
	{
		::close(_fd);
		_fd = -1;
	}

	xstr_t tmp = XSTR_CXX(_client->server());
	xstr_t xs;
	xstr_delimit_char(&tmp, '+', &xs);
	char host[256];
	xstr_copy_to(&xs, host, sizeof(host));
	int port = xstr_atoi(&tmp);
	if (port <= 0)
		port = REDIS_PORT;

	LOC_RESET(&_oloc);
	LOC_RESET(&_iloc);

	_state = ST_CONNECT;
	_fd = xnet_tcp_connect_nonblock(host, port);
	if (_fd >= 0)
	{
		xnet_set_tcp_nodelay(_fd);
		xnet_set_keepalive(_fd);
		_ib = make_iobuf(_fd, _ibuf, sizeof(_ibuf));
		_client->dispatcher()->addFd(this, _fd, XEvent::READ_EVENT | XEvent::WRITE_EVENT | XEvent::EDGE_TRIGGER);
	}
	else
	{
		_client->dispatcher()->addTask(this, RETRY_INTERVAL);
	}
}

void RConnection::event_on_task(const XEvent::DispatcherPtr& dispatcher)
{
	Lock lock(*this);
	if (_op && _fd >= 0)
	{
		dlog("RDS_ERROR", "server=%s, operation timeout", _client->server().c_str());
		do_close(dispatcher, false);
	}

	if (_fd < 0)
		reconnect();
}

void RConnection::event_on_fd(const XEvent::DispatcherPtr& dispatcher, int events)
{
	bool close = false;

	Lock lock(*this);
	if (events & XEvent::WRITE_EVENT)
	{
		if (do_write(dispatcher) < 0)
			close = true;
	}

	if (events & XEvent::READ_EVENT)
	{
		if (do_read(dispatcher) < 0)
			close = true;
	}

	if (events & XEvent::CLOSE_EVENT)
	{
		close = true;
	}

	if (close)
	{
		do_close(dispatcher, true);
	}
}

void RConnection::do_close(const XEvent::DispatcherPtr& dispatcher, bool retry)
{
	dispatcher->removeFd(this);	
	_state = ST_CLOSED;
	if (_op)
	{
		XERROR_VAR_MSG(XError, ex, "Connection closed for error");
		_op->finish(_client.get(), ex);
		_op.reset();
	}
	::close(_fd);
	_fd = -1;
	_ib.cookie = (void *)-1;
	_client->setError();

	if (retry && !_shutdown)
		dispatcher->replaceTask(this, RETRY_INTERVAL);
	else
		dispatcher->removeTask(this);
}

int RConnection::do_read(const XEvent::DispatcherPtr& dispatcher)
{
	if (_state != ST_READ)
	{
		ssize_t rc = iobuf_peek(&_ib, 1, NULL);
		if (rc == 0)
			return 0;

		if (rc < 0 || rc != -2)
			dlog("RDS_ERROR", "server=%s, iobuf_peek()=%zd, errno=%d", _client->server().c_str(), rc, errno);
		else if (rc > 0)
			dlog("RDS_FATAL", "server=%s, iobuf_peek()=%zd, errno=%d, unexpected data", _client->server().c_str(), rc, errno);
		return -1;
	}

	if (!_op)
	{
		dlog("RDS_ERROR", "server=%s, no operation waiting for answer", _client->server().c_str());
		return -1;
	}

        LOC_BEGIN(&_iloc);

	LOC_RESET(&_ctx[0].loc);
	for (_icmd = 0; _icmd < _op->cmd_num(); ++_icmd)
	{
		LOC_ANCHOR
		{
			int rc = read_one_item(0);
			if (rc < 0)
				goto error;
			else if (rc == 0)
				LOC_PAUSE(0);
		}
		_op->one_reply(_ctx[0].data);
	}

	if (_ib.len != 0)
	{
		dlog("RDS_FATAL", "More data pending for reading. This may be caused by myself bug or remote server bug");
		goto error;
	}

	if (_op)
	{
		bool ok = _op->finish(_client.get());
		_op.reset();
		if (!ok)
			goto error;
	}
	_state = ST_WAIT;
	_op = _client->connectionIdle(this);
	if (_op)
	{
		dispatcher->replaceTask(this, GIVEUP_TIMEOUT);
		_state = ST_WRITE;
		if (do_write(dispatcher) < 0)
			goto error;
	}
	else if (!_shutdown)
	{
		dispatcher->removeTask(this);
	}
	LOC_RESET(&_iloc);
	return 1;

error:
	LOC_END(&_iloc);
	return -1;
}

int RConnection::read_one_item(int level)
{
	xstr_t line;
	int64_t num;
	char ch;
	ctx_t *ctx = &_ctx[level];

        LOC_BEGIN(&ctx->loc);

	LOC_ANCHOR
	{
		ssize_t rc = iobuf_getline_xstr(&_ib, &line);
		if (rc < 0)
		{
			if (rc == -1)
				dlog("RDS_ERROR", "server=%s, iobuf_getline_xstr()=%zd, errno=%d", _client->server().c_str(), rc, errno);
			goto error;
		}
		else if (rc == 0)
		{
			LOC_PAUSE(0);
		}
	}

	if (line.len < 3 || line.data[line.len-2] != '\r')
	{
		dlog("RDS_ERROR", "server=%s, answer data not end with '\\r\\n'", _client->server().c_str());
		goto error;
	}

	line.len -= 2;
	ch = line.data[0];

	if (ch == '+')
	{
		xstr_t xs = ostk_xstr_dup(_op->ostk(), &line);
		vbs_data_set_xstr(&ctx->data, &xs, true);
	}
	else if (ch == '-')
	{
		xstr_t xs = ostk_xstr_dup(_op->ostk(), &line);
		vbs_data_set_xstr(&ctx->data, &xs, true);
	}
	else if (ch == ':')
	{
		xstr_advance(&line, 1);
		int64_t n = xstr_to_integer(&line, NULL, 10);
		vbs_data_set_integer(&ctx->data, n);
	}
	else if (ch == '$')
	{
		xstr_advance(&line, 1);
		num = xstr_atol(&line);
		if (num >= 0)
		{
			_chunk = ostk_xstr_alloc(_op->ostk(), num+2);
			_chunk_pos = 0;
			LOC_ANCHOR
			{
				ssize_t rc = iobuf_read(&_ib, _chunk.data + _chunk_pos, _chunk.len - _chunk_pos);
				if (rc < 0)
				{
					if (rc == -1)
						dlog("RDS_ERROR", "server=%s, iobuf_read()=%zd, errno=%d", _client->server().c_str(), rc, errno);
					goto error;
				}

				_chunk_pos += rc;
				if (_chunk_pos < _chunk.len)
					LOC_PAUSE(0);

				if (!xstr_end_with_cstr(&_chunk, "\r\n"))
				{
					dlog("RDS_ERROR", "server=%s, bulk reply not end with '\\r\\n'", _client->server().c_str());
					goto error;
				}
			}
			_chunk.len -= 2;

			vbs_data_set_blob(&ctx->data, _chunk.data, _chunk.len, true);
		}
		else
		{
			vbs_data_set_null(&ctx->data);
		}
	}
	else if (ch == '*')
	{
		if (level >= MAX_LEVEL - 1)
		{
			dlog("RDS_ERROR", "server=%s, multi-bulk reply nesting too deep", _client->server().c_str());
			goto error;
		}

		xstr_advance(&line, 1);
		ctx->item_num = xstr_atol(&line);
		LOC_RESET(&_ctx[level+1].loc);

		vbs_data_set_list(&ctx->data, OSTK_ALLOC_ONE(_op->ostk(), vbs_list_t));
		vbs_list_init(ctx->data.d_list, 0);
		for (ctx->item_cur = 0; ctx->item_cur < ctx->item_num; ++ctx->item_cur)
		{
			LOC_ANCHOR
			{
				int rc = read_one_item(level+1);
				if (rc < 0)
					goto error;
				else if (rc == 0)
					LOC_PAUSE(0);
			}
			vbs_litem_t *entry = OSTK_ALLOC_ONE(_op->ostk(), vbs_litem_t);
			entry->value = _ctx[level+1].data;
			vbs_list_push_back(ctx->data.d_list, entry);
		}
	}
	else
	{
		dlog("RDS_ERROR", "server=%s, invalid reply, line=%c%.*s", _client->server().c_str(), ch, XSTR_P(&line));
		goto error;
	}

	LOC_RESET(&ctx->loc);
	return 1;
error:
	LOC_END(&ctx->loc);
	return -1;
}

class AuthCallback: public RedisResultCallback
{
	RedisClientPtr _client;
public:
	AuthCallback(const RedisClientPtr& client)
		: _client(client)
	{
	}

	virtual xstr_t caller() const
	{
		return xstr_null;
	}

	virtual bool completed(const vbs_list_t& replies)
	{
		vbs_data_t *d0 = replies.first ? &replies.first->value : NULL;
                if (d0 && d0->type == VBS_STRING && xstr_equal_cstr(&d0->d_xstr, "+OK"))
			return true;

		edlog(vbs_xfmt, "RDS_AUTH", "server=%s, %p{>VBS_DATA<}", _client->server().c_str(), d0);
		return false;
	}

	virtual void exception(const std::exception& ex)
	{
	}
};


int RConnection::do_write(const XEvent::DispatcherPtr& dispatcher)
{
	if (_state == ST_CONNECT)
	{
		xstr_t password = make_xstr(_client->password());
		if (password.len)
		{
			_state = ST_WRITE;
			RedisResultCallbackPtr cb = new AuthCallback(_client);
			_op.reset(new RO_auth(cb, password));
			_client->dispatcher()->replaceTask(this, GIVEUP_TIMEOUT);
		}
		else
		{
			_state = ST_WAIT;
			if (!_idle)
			{
				_op = _client->connectionIdle(this);
				if (_op)
				{
					_client->dispatcher()->replaceTask(this, GIVEUP_TIMEOUT);
					_state = ST_WRITE;
				}
			}
			else
			{
				_client->clearError();
			}
		}
	}

	LOC_BEGIN(&_oloc);

	LOC_ANCHOR
	{
		if (!_op || _state != ST_WRITE)
			LOC_PAUSE(0);
	}

	_ov = _op->get_iovec(&_ov_num);

	LOC_ANCHOR
	{
		ssize_t rc = xnet_writev_nonblock(_fd, _ov, _ov_num);
		if (rc < 0)
		{
			if (rc == -1)
				dlog("RDS_ERROR", "server=%s, xnet_writev_nonblock()=%zd, fd=%d, errno=%d", _client->server().c_str(), rc, _fd, errno);
			goto error;
		}

		_ov_num = xnet_adjust_iovec(&_ov, _ov_num, rc);
		if (_ov_num > 0)
		{
			LOC_PAUSE(0);
		}
		_ov = NULL;

	}
	_state = ST_READ;
	LOC_RESET(&_oloc);
	return 1;

error:
	LOC_END(&_oloc);
	return -1;
}


RedisClient::RedisClient(const XEvent::DispatcherPtr& dispatcher, const std::string& service,
		 const std::string& server, const std::string& password, size_t maxConnection)
	: _dispatcher(dispatcher), _service(service), 
	_server(server), _password(password), _max_con(maxConnection)
{
	_shutdown = false;	
	_error = false;
	if (_max_con <= 0 || _max_con > 1024)
		_max_con = DEFAULT_CON_NUM;
	_err_con = 0;
	_istack.reserve(_max_con);
	_cons.reserve(_max_con);
}

RedisClient::~RedisClient()
{
	while (!_queue.empty())
	{
		RedisOperationPtr op = _queue.front();
		_queue.pop_front();
		XERROR_VAR_MSG(XError, ex, "RedisClient destroied");
		op->finish(this, ex);
	}
}

void RedisClient::process(const RedisOperationPtr& op)
{
	RConnectionPtr con;
	{
		Lock lock(*this);
		if (_shutdown)
		{
			XERROR_VAR_MSG(XError, ex, "RedisClient shutdown");
			op->finish(this, ex);
			return;
		}

		while (!_istack.empty())
		{
			RConnectionPtr c = _istack.back();
			c->setIdle(false);
			_istack.pop_back();
			if (c->ok())
			{
				con = c;
				break;
			}
		}

		if (!con)
		{
			_queue.push_back(op);

			if (_cons.size() < (size_t)_max_con)
			{
				RConnectionPtr c(new RConnection(this));
				c->reconnect(); 
				_cons.push_back(c);
			}
		}
	}

	if (con)
	{
		con->process(op);
	}
}

void RedisClient::shutdown()
{
	Lock lock(*this);
	_shutdown = true;
	_istack.clear();

	size_t size = _cons.size();
	for (size_t i = 0; i < size; ++i)
	{
		_cons[i]->shutdown();
	}

	_cons.clear();
}

void RedisClient::setError()
{
	bool available = false;

	Lock lock(*this);
	++_err_con;

	for (size_t i = 0; i < _cons.size(); ++i)
	{
		if (!_cons[i]->closed())
		{
			available = true;
			break;
		}
	}

	if (!available)
	{
		_error = true;
		while (!_queue.empty())
		{
			RedisOperationPtr op = _queue.front();
			_queue.pop_front();
			XERROR_VAR_MSG(XError, ex, "No Redis server available");
			op->finish(this, ex);
		}
	}
}

void RedisClient::clearError()
{
	Lock lock(*this);
	_error = false;
}

RedisOperationPtr RedisClient::connectionIdle(RConnection* con)
{
	RedisOperationPtr op;

	Lock lock(*this);
	// clear error
	_error = false;

	if (_queue.size())
	{
		op = _queue.front();
		_queue.pop_front();
	}
	else if (!_shutdown)
	{
		con->setIdle(true);
		_istack.push_back(RConnectionPtr(con));
	}

	return op;
}

