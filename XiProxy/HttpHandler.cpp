#include "HttpHandler.h"
#include "HttpResponse.h"
#include "xic/EngineImp.h"
#include "dlog/dlog.h"
#include "xslib/xlog.h"
#include "xslib/xbuf.h"
#include "xslib/vbs_json.h"
#include "xslib/urlparse.h"


HttpHandler::HttpHandler(const xic::EnginePtr& engine, const xic::AdapterPtr& adapter)
	: _daemon(NULL), _engine(engine), _adapter(adapter)
{
	SettingPtr setting = _engine->setting();
	_port = setting->getInt("XiProxy.Http.Port", 9988);
	_connectionTimeout = setting->getInt("XiProxy.Http.Connection.Timeout", 60);
	_connectionLimit = setting->getInt("XiProxy.Http.Connection.Limit", 1024);
	_threadPoolSize = setting->getInt("XiProxy.Http.ThreadPool.Size", 2);
}

HttpHandler::~HttpHandler()
{
	if (_daemon)
		stop();
}

static int answer_to_connection (void *cls, struct MHD_Connection *con,
		const char *url, const char *method, const char *version, 
		const char *data, size_t *data_size, void **con_cls)
{
	HttpHandler *handler = (HttpHandler *)cls;
	return handler->process(con, url, method, version, data, data_size, con_cls);
}

static void request_completed (void *cls, struct MHD_Connection *con,
		void **con_cls, enum MHD_RequestTerminationCode toe)
{
	HttpHandler *handler = (HttpHandler *)cls;
	return handler->complete(con, con_cls, toe);
}

void HttpHandler::start()
{
	if (_daemon)
		throw XERROR_MSG(XError, "MHD_Daemon already started");

	_daemon = MHD_start_daemon(MHD_USE_EPOLL_INTERNALLY_LINUX_ONLY | MHD_USE_SUSPEND_RESUME,
			_port,
			NULL, NULL,
			&answer_to_connection, this,
			MHD_OPTION_NOTIFY_COMPLETED, request_completed, this,
			MHD_OPTION_CONNECTION_TIMEOUT, (unsigned int)_connectionTimeout,
			MHD_OPTION_CONNECTION_LIMIT, (unsigned int)_connectionLimit,
			MHD_OPTION_THREAD_POOL_SIZE, (unsigned int)_threadPoolSize,
			MHD_OPTION_END);

	if (!_daemon)
		throw XERROR_MSG(XError, "MHD_start_daemon() failed");
}

void HttpHandler::stop()
{
	if (!_daemon)
		throw XERROR_MSG(XError, "MHD_Daemon not started");

	MHD_stop_daemon(_daemon);
	_daemon = NULL;
}

class HttpFakeCurrent: public xic::CurrentI
{
	friend class HttpFakeWaiter;
	struct MHD_Connection *_con;
public:
	HttpFakeCurrent(struct MHD_Connection *con, const xic::QuestPtr& q)
		: CurrentI(NULL, q.get()), _con(con)
	{
	}

	virtual xic::WaiterPtr asynchronous() const;
};

class HttpFakeWaiter: public xic::WaiterI
{
	struct MHD_Connection *_con;
public:
	HttpFakeWaiter(const HttpFakeCurrent& r)
		: xic::WaiterI(r), _con(r._con)
	{
	}

	virtual bool responded() const 		{ return false; }

        virtual void response(const xic::AnswerPtr& answer, bool trace);
};

xic::WaiterPtr HttpFakeCurrent::asynchronous() const
{
	if (!_waiter)
	{
		_waiter.reset(new HttpFakeWaiter(*this));
	}
	return _waiter;
}

void HttpFakeWaiter::response(const xic::AnswerPtr& answer, bool trace)
{
	xstr_t a = answer->args_xstr();
	std::ostringstream ss;
	vbs_to_json(a.data, a.len, ostream_xio.write, (std::ostream*)&ss, 0);
	std::string out = ss.str();

	MHD_Response *response = MHD_create_response_from_buffer(out.length(), (void *)out.data(), MHD_RESPMEM_MUST_COPY);
	MHD_add_response_header(response, MHD_HTTP_HEADER_CONTENT_TYPE, "application/json");
	int code = MHD_HTTP_OK;
	if (answer->status())
	{
		code = MHD_HTTP_BAD_REQUEST;
		xic::VDict args = answer->args();
		xstr_t exname = args.getXstr("exname");
		if (xstr_equal_cstr(&exname, "xic.ServiceNotFoundException") || xstr_equal_cstr(&exname, "xic.MethodNotFoundException"))
			code = MHD_HTTP_NOT_FOUND;
	}
	MHD_queue_response(_con, code, response);
	MHD_destroy_response(response);
	MHD_resume_connection(_con);
}

int HttpHandler::_request(struct MHD_Connection *con, const char *url, const xic::QuestPtr& q)
{
	xstr_t tmp = XSTR_C(url);
	struct urlpart part;
	if (urlparse(&tmp, &part) < 0)
		return http_respond_bad_request(con);

	xstr_t service, method;
	tmp = part.path;
	xstr_delimit_char(&tmp, '/', NULL);
	xstr_delimit_char(&tmp, '/', &service);
	method = tmp;
	q->setTxid(-1);
	q->setService(service);
	q->setMethod(method);

	MHD_suspend_connection(con);
	xic::AnswerPtr answer;
	HttpFakeCurrent fake_current(con, q);
	try {
		xic::ServantPtr srv = _adapter->findServant(make_string(service));
		if (!srv)
		{
			srv = _adapter->getDefaultServant();
			if (!srv)
				throw XERROR_FMT(xic::ServiceNotFoundException, "%.*s", XSTR_P(&service));
		}

		answer = srv->process(q, fake_current);
	}
	catch (XError& ex)
	{
		dlog("ERROR", "%s", ex.what());
		answer = xic::except2answer(ex, method, service, "");
	}

	if (answer != xic::ASYNC_ANSWER)
	{
		xic::WaiterPtr waiter = fake_current.asynchronous();
		waiter->response(answer);
	}
	return MHD_YES;
}

static int querystring_iterator(void *cls, enum MHD_ValueKind kind, const char *key, const char *value)
{
	xic::QuestWriter *qw = (xic::QuestWriter*)cls;
	qw->param(key, value);
	return MHD_YES;
}

typedef struct
{
	xbuf_t xb;
	bool processed;
} scene_t;

int HttpHandler::process(struct MHD_Connection *con, const char *url, 
		const char *method, const char *version,
		const char *data, size_t *data_size, void **ptr)
{
	if (strcmp(method, MHD_HTTP_METHOD_POST) == 0)
	{
		if (*ptr == NULL)
		{
			ssize_t size = -1;
			const char *content_length = MHD_lookup_connection_value(con, MHD_HEADER_KIND, MHD_HTTP_HEADER_CONTENT_LENGTH);
			if (content_length)
			{
				char *end;
				size = strtoul(content_length, &end, 10);
				if (size < 0 || *end)
					return http_respond_bad_request(con);
			}
			else
			{
				size = *data_size;
				if (size < 4096)
					size = 4096;
			}

			scene_t *sn = XS_CALLOC_ONE(scene_t);
			if (!sn)
			{
				return http_respond_internal_server_error(con);
			}

			*ptr = sn;
			if (size > 0)
			{
				sn->xb.data = (unsigned char *)malloc(size);
				if (!sn->xb.data)
				{
					return http_respond_internal_server_error(con);
				}
				memcpy(sn->xb.data, data, *data_size);
				sn->xb.capacity = size;
				sn->xb.len = *data_size;
				*data_size = 0;
			}
		}
		else if (*data_size)
		{
			scene_t *sn = (scene_t *)*ptr;
			ssize_t size = sn->xb.len + *data_size;
			if (size > sn->xb.capacity)
			{
				unsigned char *data = (unsigned char *)realloc(sn->xb.data, size);
				if (!data)
					return http_respond_internal_server_error(con);

				sn->xb.data = data;
				sn->xb.capacity = size;
			}

			memcpy(sn->xb.data + sn->xb.len, data, *data_size);
			sn->xb.len = size;
			*data_size = 0;
		}
		else
		{
			scene_t *sn = (scene_t *)*ptr;
			if (!sn->processed)
			{
				sn->processed = true;
				if (sn->xb.len)
				{
					xic::QuestPtr q = xic::Quest::create();
					int rc = json_to_vbs(sn->xb.data, sn->xb.len, rope_xio.write, q->args_rope(), 0);
					free(sn->xb.data);
					sn->xb.data = NULL;
					sn->xb.len = 0;
					if (rc < 0)
						return http_respond_bad_request(con, "Posted data is not a valid JSON string");
					return _request(con, url, q);
				}
				else
				{
					return http_respond_bad_request(con, "No data posted");
				}
			}

			dlog("XXX", "Can't reach here!");
			return MHD_NO;
		}
	}
	else if (strcmp(method, MHD_HTTP_METHOD_GET) == 0)
	{
		xic::QuestWriter qw("");
		MHD_get_connection_values(con, MHD_GET_ARGUMENT_KIND, querystring_iterator, &qw);
		xic::QuestPtr q = qw.take();
		return _request(con, url, q);
	}
	else
	{
		return http_respond_method_not_allowed(con);
	}

	return MHD_YES;
}

void HttpHandler::complete(struct MHD_Connection *con, void **ptr, enum MHD_RequestTerminationCode toe)
{
	scene_t *sn = (scene_t *)(*ptr);
	if (sn)
	{
		free(sn);
	}
}

