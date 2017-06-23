#include "HttpHandler.h"
#include "Response.h"
#include "xslib/xlog.h"
#include "dlog/dlog.h"


HttpHandler::HttpHandler(const xic::EnginePtr& engine)
	: _daemon(NULL), _engine(engine)
{
	SettingPtr setting = _engine->setting();
	std::string policy_file = setting->wantPathname("http.policy");
	_pm.reset(new PolicyManager(engine, policy_file));
	_port = setting->getInt("http.port", 8888);
	_connectionTimeout = setting->getInt("http.connection.timeout", 60);
	_connectionLimit = setting->getInt("http.connection.limit", 1024);
	_threadPoolSize = setting->getInt("http.threadpool.size", 32);
}

HttpHandler::~HttpHandler()
{
	if (_daemon)
		stop();
}

static int answer_to_connection (void *cls, struct MHD_Connection *connection,
		const char *url, const char *method, const char *version, 
		const char *data, size_t *data_size, void **con_cls)
{
	HttpHandler *handler = (HttpHandler *)cls;
	return handler->process(connection, url, method, version, data, data_size, con_cls);
}

static void request_completed (void *cls, struct MHD_Connection *connection,
		void **con_cls, enum MHD_RequestTerminationCode toe)
{
	HttpHandler *handler = (HttpHandler *)cls;
	return handler->complete(connection, con_cls, toe);
}

void HttpHandler::start()
{
	if (_daemon)
		throw XERROR_MSG(XError, "MHD_Daemon already started");

	_daemon = MHD_start_daemon(MHD_USE_SELECT_INTERNALLY | MHD_USE_EPOLL_LINUX_ONLY | MHD_USE_EPOLL_TURBO,
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

struct Scene
{
	Policy *policy;
	void *ctx;

	Scene(const PolicyPtr& p)
		: policy(p.get()), ctx(NULL)
	{
		policy->xref_inc();
	}

	~Scene()
	{
		policy->xref_dec();
	}
};

int HttpHandler::process(struct MHD_Connection *connection, const char *url, 
		const char *method, const char *version,
		const char *data, size_t *data_size, void **ptr)
{
	Scene *s = (Scene *)(*ptr);
	if (!s)
	{
		dlog("HTTP", "%s %s %s", method, url, version);
		PolicyPtr policy = _pm->getPolicy(url);
		if (!policy)
		{
			return respond_not_found(connection);
		}

		s = new Scene(policy);
		*ptr = s;
	}

	try {
		return s->policy->process(connection, url, method, version, data, data_size, &s->ctx);
	}
	catch (XArgumentError& ex)
	{
		dlog("ERROR", "%s", ex.what());
		std::string msg = ex.message();
		MHD_Response *response = MHD_create_response_from_buffer(msg.length(), (void *)msg.data(), MHD_RESPMEM_MUST_COPY);
		int ret = MHD_queue_response(connection, MHD_HTTP_BAD_REQUEST, response);
		MHD_destroy_response(response);
		return ret;
	}
	catch (XError& ex)
	{
		dlog("ERROR", "%s", ex.what());
		return respond_internal_server_error(connection);
	}
}

void HttpHandler::complete(struct MHD_Connection *connection, void **ptr,
		enum MHD_RequestTerminationCode toe)
{
	Scene *s = (Scene *)(*ptr);
	if (s)
	{
		try {
			s->policy->complete(connection, &s->ctx, toe);
		}
		catch (XError& ex)
		{
			dlog("ERROR", "%s", ex.what());
		}
		delete s;
	}
}


