#ifndef MClient_h_
#define MClient_h_

#include "xslib/XRefCount.h"
#include "xslib/XLock.h"
#include "xslib/XEvent.h"
#include "MOperation.h"
#include <string>
#include <vector>
#include <set>
#include <deque>

class MClient;
class MConnection;
typedef XPtr<MClient> MClientPtr;
typedef XPtr<MConnection> MConnectionPtr;


class MClient: virtual public XRefCount, private XMutex
{
public:
	MClient(const XEvent::DispatcherPtr& dispatcher, const std::string& service, const std::string& server, size_t maxConnection);
	virtual ~MClient();

	XEvent::DispatcherPtr dispatcher() const 	{ return _dispatcher; }
	const std::string& service() const 		{ return _service; }
	const std::string& server() const 		{ return _server; }
	bool error() const				{ return _error; }

	void process(const MOperationPtr& op);
	void start();
	void shutdown();

	void connectionError(MConnection* con);
	MOperationPtr connectionIdle(MConnection* con);

private:
	int on_reap_timer();
	int on_retry_timer();

private:
	XEvent::DispatcherPtr _dispatcher;
	std::string _service;
	std::string _server;
	bool _shutdown;
	bool _error;
	int _err_count;
	int _max_con;
	int _idle;
	int64_t _last_con_time;
	std::deque<MOperationPtr> _queue;
	std::vector<MConnectionPtr> _istack;
	std::set<MConnectionPtr> _cons;
};


#endif
