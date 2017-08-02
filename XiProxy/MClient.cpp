#include "MClient.h"
#include "dlog/dlog.h"
#include "xslib/XEvent.h"
#include "xslib/xnet.h"
#include "xslib/loc.h"
#include "xslib/iobuf.h"
#include <assert.h>
#include <errno.h>
#include <unistd.h>

#define MEMCACHE_PORT	11211
#define DEFAULT_CON_NUM	6

#define CONNECT_TIMEOUT		(2*1000)
#define OPERATION_TIMEOUT	(2*1000)
#define SHUTDOWN_TIMEOUT	(5*1000)
#define CONNECT_INTERVAL	(1*1000)
#define RETRY_INTERVAL		(15*1000)
#define REAP_INTERVAL		(300*1000)

class MConnection: public XEvent::FdHandler, public XEvent::TaskHandler, private XMutex
{
public:
	MConnection(MClient* mclient);
	virtual ~MConnection();

	void process(const MOperationPtr& op);
	void shutdown();
	bool ok() const				{ return (!_shutdown && _state == ST_WAIT); }
	void connect();

	virtual void event_on_fd(const XEvent::DispatcherPtr& dispatcher, int events);
	virtual void event_on_task(const XEvent::DispatcherPtr& dispatcher);

private:
	int do_read(const XEvent::DispatcherPtr& dispatcher);
	int do_write(const XEvent::DispatcherPtr& dispatcher);
	void do_close(const XEvent::DispatcherPtr& dispatcher);
	int on_shutdown_timeout();

private:
	MClientPtr _mclient;
	MOperationPtr _op;
	int _fd;

	bool _shutdown;

	enum {
		ST_CONNECT,
		ST_WAIT,
		ST_WRITE,
		ST_READ,
		ST_CLOSED,
	} _state;

        loc_t _iloc;
        iobuf_t _ib;
        unsigned char _ibuf[1024];
        ssize_t _ipos;
	MValue _mv;

        loc_t _oloc;
        struct iovec *_ov;
        int _ov_num;
};

class VersionCallback: public MCallback
{
	MClientPtr _mclient;
public:
	VersionCallback(const MClientPtr& mclient)
		: MCallback(MOC_VERSION), _mclient(mclient)
	{
	}

	virtual xstr_t caller() const
	{
		return xstr_null;
	}

	virtual void received(int64_t value)
	{
	}

	virtual void received(const MValue values[], size_t n, bool cache,
			void (*cleanup)(void *), void *cleanup_arg)
	{
	}

	virtual void completed(bool ok, bool zip)
	{
	}
};


MConnection::MConnection(MClient* mclient)
	: _mclient(mclient)
{
	_shutdown = false;
	_fd = -1;
}

MConnection::~MConnection()
{
	if (_fd >= 0)
		::close(_fd);
}

void MConnection::process(const MOperationPtr& op)
{
	Lock lock(*this);
	if (_state != ST_WAIT)
		throw XERROR_MSG(XError, "MConnection not ready");

	assert(!_op && op);
	_op = op;
	_state = ST_WRITE;

	XEvent::DispatcherPtr dispatcher = _mclient->dispatcher();
	if (do_write(dispatcher) < 0)
		do_close(dispatcher);
	else
		dispatcher->replaceTask(this, OPERATION_TIMEOUT);
}

void MConnection::shutdown()
{
	Lock lock(*this);
	_shutdown = true;
	_mclient->dispatcher()->addTask(XEvent::TaskHandler::create(this, &MConnection::on_shutdown_timeout), SHUTDOWN_TIMEOUT);
}

int MConnection::on_shutdown_timeout()
{
	Lock lock(*this);
	do_close(_mclient->dispatcher());
	return 0;
}

void MConnection::connect()
{
	if (_fd >= 0)
	{
		::close(_fd);
		_fd = -1;
	}

	xstr_t tmp = XSTR_CXX(_mclient->server());
	xstr_t xs;
	xstr_delimit_char(&tmp, '+', &xs);
	char host[256];
	xstr_copy_cstr(&xs, host, sizeof(host));
	int port = xstr_atoi(&tmp);
	if (port <= 0)
		port = MEMCACHE_PORT;

	LOC_RESET(&_iloc);
	LOC_RESET(&_oloc);

	_state = ST_CONNECT;
	_fd = xnet_tcp_connect_nonblock(host, port);
	if (_fd >= 0)
	{
		xnet_set_tcp_nodelay(_fd);
		xnet_set_keepalive(_fd);
		_ib = make_iobuf(_fd, _ibuf, sizeof(_ibuf));
		_mclient->dispatcher()->addFd(this, _fd, XEvent::READ_EVENT | XEvent::WRITE_EVENT | XEvent::EDGE_TRIGGER);
		_mclient->dispatcher()->addTask(this, CONNECT_TIMEOUT);
	}
	else
	{
		_mclient->connectionError(this);
	}
}

void MConnection::event_on_task(const XEvent::DispatcherPtr& dispatcher)
{
	Lock lock(*this);
	if (_fd >= 0)
	{
		const char *op = (_state == ST_CONNECT) ? "connecting"
				: (_op && _op->category() == MOC_VERSION) ? "version"
				: "operation";
		dlog("MC_ERROR", "server=%s, %s timeout", _mclient->server().c_str(), op);
		do_close(dispatcher);
	}
}

void MConnection::event_on_fd(const XEvent::DispatcherPtr& dispatcher, int events)
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
		do_close(dispatcher);
	}
}

void MConnection::do_close(const XEvent::DispatcherPtr& dispatcher)
{
	dispatcher->removeFd(this);
	_state = ST_CLOSED;
	if (_op)
	{
		_op->finish(_mclient, false);
		_op.reset();
	}
	::close(_fd);
	_fd = -1;
	_ib.cookie = (void *)-1;
	_mclient->connectionError(this);
}

int MConnection::do_read(const XEvent::DispatcherPtr& dispatcher)
{
	xstr_t line;
	char ch, ch1;
	bool ok = false;

        LOC_BEGIN(&_iloc);
	LOC_ANCHOR
	{
		ssize_t rc = iobuf_getline_xstr(&_ib, &line);
		if (rc < 0)
		{
			if (rc == -1)
				dlog("MC_ERROR", "server=%s, iobuf_getline_xstr()=%zd, errno=%d", _mclient->server().c_str(), rc, errno);
			goto error;
		}
		else if (rc == 0)
		{
			LOC_PAUSE(0);
		}

		if (_state != ST_READ)
		{
			dlog("MC_ERROR", "server=%s, unexpected _state(%d), expect ST_READ, line=%.*s", _mclient->server().c_str(), _state, XSTR_P(&line));
			goto error;
		}

		if (rc < 3 || line.data[rc-2] != '\r')
		{
			dlog("MC_ERROR", "server=%s, answer data not end with '\\r\\n'", _mclient->server().c_str());
			goto error;
		}

		line.len = rc - 2;
	}

	if (!_op)
	{
		dlog("MC_ERROR", "server=%s, no operation waiting for answer, line=%.*s", _mclient->server().c_str(), XSTR_P(&line));
		goto error;
	}

	ch = line.data[0];
	ch1 = line.len > 1 ? line.data[1] : 0;
	if ((ch == 'E' && ch1 == 'R' && xstr_equal_cstr(&line, "ERROR"))
		|| (ch == 'C' && ch1 == 'L' && xstr_start_with_cstr(&line, "CLIENT_ERROR")))
	{
		int iov_num = 0;
		struct iovec *iov = _op->get_iovec(&iov_num);
		dlog("MC_ERROR", "server=%s, %.*s\ncmd=%.*s", _mclient->server().c_str(), XSTR_P(&line), (int)iov[0].iov_len, (char *)iov[0].iov_base);
		goto finish;
	}
	else if (ch == 'S' && ch1 == 'E' && xstr_start_with_cstr(&line, "SERVER_ERROR"))
	{
		int iov_num = 0;
		struct iovec *iov = _op->get_iovec(&iov_num);
		dlog("MC_ERROR", "server=%s, %.*s\ncmd=%.*s", _mclient->server().c_str(), XSTR_P(&line), (int)iov[0].iov_len, (char *)iov[0].iov_base);
		goto error;
	}

	switch (_op->category())
	{
	case MOC_VERSION:
		if (ch == 'V')		// VERSION
		{
			ok = true;
		}
		else
		{
			dlog("MC_PROTO", "%.*s", XSTR_P(&line));
			goto error;
		}
		break;

	case MOC_STORE:
		if (ch == 'S')		// STORED
		{
			ok = true;
		}
		else if (ch == 'N')	// NOT_STORED
		{
			ok = false;
		}
		else
		{
			dlog("MC_PROTO", "%.*s", XSTR_P(&line));
			goto error;
		}
		break;

	case MOC_CAS:
		if (ch == 'S')		// STORED
		{
			ok = true;
		}
		else if (ch == 'E') 	// EXISTS
		{
			ok = false;
		}
		else if (ch == 'N')	// NOT_FOUND
		{
			ok = false;
		}
		else
		{
			dlog("MC_PROTO", "%.*s", XSTR_P(&line));
			goto error;
		}
		break;

	case MOC_DELETE:
		if (ch == 'D')		// DELETED
		{
			ok = true;
		}
		else if (ch == 'N')	// NOT_FOUND
		{
			ok = false;
		}
		else
		{
			dlog("MC_PROTO", "%.*s", XSTR_P(&line));
			goto error;
		}
		break;

	case MOC_COUNT:
		if (ch >= '0' && ch <= '9')
		{
			ok = true;
			_op->callback()->received(xstr_to_integer(&line, NULL, 10));
		}
		else if (ch == 'N')		// NOT_FOUND
		{
			ok = false;
		}
		else
		{
			dlog("MC_PROTO", "%.*s", XSTR_P(&line));
			goto error;
		}
		break;

	case MOC_GET:
	case MOC_GETMULTI:
		// see below
		break;

	default:
		dlog("MC_PROTO", "%.*s", XSTR_P(&line));
		goto error;
	}

	if (_op->category() == MOC_GET || _op->category() == MOC_GETMULTI)
	{
		while (ch == 'V')	// VALUE
		{
			{
				/* VALUE <key> <flags> <bytes> [<cas unique>]\r\n */
				xstr_t xs = line;
				xstr_t cmd, key, flags, bytes, cas;
				xstr_delimit_in_space(&xs, &cmd);
				xstr_delimit_in_space(&xs, &key);
				xstr_delimit_in_space(&xs, &flags);
				xstr_delimit_in_space(&xs, &bytes);
				xstr_delimit_in_space(&xs, &cas);
				if (!xstr_equal_cstr(&cmd, "VALUE") || xs.len != 0)
				{
					dlog("MC_PROTO", "%.*s", XSTR_P(&line));
					goto error;
				}

				_mv.key = ostk_xstr_dup(_op->ostk(), &key);
				_mv.value.len = xstr_to_integer(&bytes, NULL, 10) + 2;
				_mv.value.data = (unsigned char *)ostk_alloc(_op->ostk(), _mv.value.len);
				_mv.revision = xstr_to_integer(&cas, NULL, 10);
				_mv.flags = xstr_to_integer(&flags, NULL, 10);
				_mv.zip = bool(_mv.flags & FLAG_LZ4_ZIP);
			}

			_ipos = 0;
			LOC_ANCHOR
			{
				ssize_t rc = iobuf_read(&_ib, _mv.value.data + _ipos, _mv.value.len - _ipos);
				if (rc < 0)
				{
					if (rc == -1)
						dlog("MC_ERROR", "server=%s, iobuf_read()=%zd, errno=%d", _mclient->server().c_str(), rc, errno);
					goto error;
				}

				_ipos += rc;
				if (_ipos < _mv.value.len)
					LOC_PAUSE(0);

				if (!xstr_end_with_cstr(&_mv.value, "\r\n"))
				{
					dlog("MC_ERROR", "server=%s, answer data not end with '\\r\\n'", _mclient->server().c_str());
					goto error;
				}
				_mv.value.len -= 2;
				_op->appendMValue(_mv);
			}

			LOC_ANCHOR
			{
				ssize_t rc = iobuf_getline_xstr(&_ib, &line);
				if (rc < 0)
				{
					if (rc == -1)
						dlog("MC_ERROR", "server=%s, iobuf_getline_xstr()=%zd, errno=%d", _mclient->server().c_str(), rc, errno);
					goto error;
				}
				else if (rc == 0)
				{
					LOC_PAUSE(0);
				}

				if (rc < 3 || line.data[rc-2] != '\r')
				{
					dlog("MC_ERROR", "server=%s, answer data not end with '\\r\\n'", _mclient->server().c_str());
					goto error;
				}

				line.len = rc - 2;
				ch = line.data[0];
			}
		}

		if (ch == 'E')		// END
		{
			if (_op)
				_op->informCallback();
			ok = true;
		}
		else
		{
			dlog("MC_PROTO", "%.*s", XSTR_P(&line));
			goto error;
		}
	}

finish:
	if (_ib.len != 0)
	{
		dlog("MC_FATAL", "More data pending for reading. This may be caused by myself bug or memcached bug");
		goto error;
	}

	if (_op)
	{
		_op->finish(_mclient, ok);
		_op.reset();
	}
	_state = ST_WAIT;
	_op = _mclient->connectionIdle(this);
	if (_op)
	{
		dispatcher->replaceTask(this, OPERATION_TIMEOUT);
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

int MConnection::do_write(const XEvent::DispatcherPtr& dispatcher)
{
	if (_state == ST_CONNECT)
	{
		_state = ST_WRITE;
		_op.reset(new MO_version(MCallbackPtr(new VersionCallback(_mclient))));
		_mclient->dispatcher()->replaceTask(this, OPERATION_TIMEOUT);
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
				dlog("MC_ERROR", "server=%s, xnet_writev_nonblock()=%zd, fd=%d, errno=%d", _mclient->server().c_str(), rc, _fd, errno); 
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


MClient::MClient(const XEvent::DispatcherPtr& dispatcher, const std::string& service, const std::string& server, size_t maxConnection)
	: _dispatcher(dispatcher), _service(service), _server(server), _max_con(maxConnection)
{
	_shutdown = false;	
	_error = false;
	_err_count = 0;
	if (_max_con <= 0 || _max_con > 1024)
		_max_con = DEFAULT_CON_NUM;
	_idle = 0;
	_last_con_time = 0;
	_istack.reserve(_max_con);
}

MClient::~MClient()
{
	while (!_queue.empty())
	{
		MOperationPtr op = _queue.front();
		_queue.pop_front();
		op->finish(MClientPtr(this), false);
	}
}

void MClient::process(const MOperationPtr& op)
{
	MConnectionPtr con;
	{
		Lock lock(*this);
		if (_shutdown || _error)
		{
			op->finish(MClientPtr(this), false);
			return;
		}

		while (!_istack.empty())
		{
			MConnectionPtr c = _istack.back();
			_istack.pop_back();
			if (c->ok())
			{
				con = c;
				break;
			}
		}

		if (_idle > (int)_istack.size())
			_idle = _istack.size();

		if (!con)
		{
			_queue.push_back(op);

			if (_cons.size() < (size_t)_max_con)
			{
				int64_t now = _dispatcher->msecMonotonic();
				if (_last_con_time < now - CONNECT_INTERVAL)
				{
					_last_con_time = now;
					MConnectionPtr c(new MConnection(this));
					c->connect(); 
					_cons.insert(c);
				}
			}
		}
	}

	if (con)
	{
		con->process(op);
	}
}

void MClient::start()
{
	_dispatcher->addTask(XEvent::TaskHandler::create(this, &MClient::on_reap_timer), REAP_INTERVAL);
}

void MClient::shutdown()
{
	std::set<MConnectionPtr> cons;
	{
		Lock lock(*this);
		_shutdown = true;
		_istack.clear();
		_cons.swap(cons);
	}

	for (std::set<MConnectionPtr>::iterator iter = cons.begin(); iter != cons.end(); ++iter)
	{
		(*iter)->shutdown();
	}
}

int MClient::on_reap_timer()
{
	MConnectionPtr con;
	{
		Lock lock(*this);
		if (_idle && _cons.size() > 1)
		{
			std::set<MConnectionPtr>::iterator iter = _cons.begin();
			con = *iter;
			_cons.erase(iter);
		}
		_idle = _istack.size();
	}

	if (con)
		con->shutdown();

	return _shutdown ? 0 : REAP_INTERVAL;
}

int MClient::on_retry_timer()
{
	MConnectionPtr con;
	{
		Lock lock(*this);
		if (_cons.empty() && !_shutdown)
		{
			_last_con_time = _dispatcher->msecMonotonic();
			con.reset(new MConnection(this));
			_cons.insert(con);
		}
	}

	if (con)
		con->connect();

	return 0;
}

void MClient::connectionError(MConnection* con)
{
	std::deque<MOperationPtr> ops;
	{
		Lock lock(*this);
		_cons.erase(MConnectionPtr(con));
		++_err_count;
		if ((_cons.empty() || _err_count >= _max_con) && !_shutdown)
		{
			int retry_interval = RETRY_INTERVAL;
			if (_error)
			{
				dlog("MC_ALERT", "server=%s", _server.c_str());
			}
			else
			{
				retry_interval = (random() % RETRY_INTERVAL) / 2 + 1;
			}
			_error = true;
			_istack.clear();
			_queue.swap(ops);
			_dispatcher->addTask(XEvent::TaskHandler::create(this, &MClient::on_retry_timer), retry_interval);
		}
	}

	while (!ops.empty())
	{
		MOperationPtr op = ops.front();
		ops.pop_front();
		op->finish(MClientPtr(this), false);
	}
}

MOperationPtr MClient::connectionIdle(MConnection* con)
{
	MOperationPtr op;

	Lock lock(*this);
	_error = false;
	_err_count = 0;

	if (_queue.size())
	{
		op = _queue.front();
		_queue.pop_front();
	}
	else if (!_shutdown)
	{
		_istack.push_back(MConnectionPtr(con));
	}

	return op;
}

