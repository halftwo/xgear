#ifndef RedisClient_h_
#define RedisClient_h_

#include "xslib/XRefCount.h"
#include "xslib/XLock.h"
#include "xslib/XEvent.h"
#include "RedisOp.h"
#include <string>
#include <vector>
#include <stack>
#include <deque>

class RedisClient;
class RConnection;
typedef XPtr<RedisClient> RedisClientPtr;
typedef XPtr<RConnection> RConnectionPtr;


class RedisClient: virtual public XRefCount, private XMutex
{
public:
	RedisClient(const XEvent::DispatcherPtr& dispatcher, const std::string& service, 
			const std::string& server, const std::string& password, size_t maxConnection);
	virtual ~RedisClient();

	XEvent::DispatcherPtr dispatcher() const 	{ return _dispatcher; }
	const std::string& service() const 		{ return _service; }
	const std::string& server() const 		{ return _server; }
	const std::string& password() const 		{ return _password; }
	bool error() const				{ return _error; }

	void process(const RedisOperationPtr& op);
	void shutdown();

	RedisOperationPtr connectionIdle(RConnection* con);
	void setError();
	void clearError();

private:
	XEvent::DispatcherPtr _dispatcher;
	std::string _service;
	std::string _server;
	std::string _password;
	bool _shutdown;
	bool _error;
	int _max_con;
	int _err_con;
	std::deque<RedisOperationPtr> _queue;
	std::vector<RConnectionPtr> _istack;
	std::vector<RConnectionPtr> _cons;
};


#endif
