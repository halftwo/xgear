#ifndef HttpHandler_h_
#define HttpHandler_h_

#include "Policy.h"
#include "xslib/XRefCount.h"
#include "xslib/Setting.h"
#include "xic/Engine.h"
#include <microhttpd.h>

class HttpHandler;
typedef XPtr<HttpHandler> HttpHandlerPtr;


class HttpHandler: virtual public XRefCount
{
	struct MHD_Daemon* _daemon;
	xic::EnginePtr _engine;
	int _port;
	int _connectionTimeout;
	int _connectionLimit;
	int _threadPoolSize;
	PolicyManagerPtr _pm;
public:
	HttpHandler(const xic::EnginePtr& engine);
	virtual ~HttpHandler();

	void start();
	void stop();

	int process(struct MHD_Connection *connection, const char *url, 
			const char *method, const char *version,
			const char *data, size_t *data_size, void **ptr);

	void complete(struct MHD_Connection *connection, void **con_cls,
			enum MHD_RequestTerminationCode toe);
};


#endif
