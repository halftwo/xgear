#ifndef RedisGroup_h_
#define RedisGroup_h_

#include "xslib/XRefCount.h"
#include "xslib/xstr.h"
#include "xslib/HSequence.h"
#include "xslib/UniquePtr.h"
#include "RedisClient.h"
#include <string>
#include <vector>
#include <map>


class RedisGroup;
typedef XPtr<RedisGroup> RedisGroupPtr;

class RGroupMgetCallback: public XRefCount
{
public:
	virtual xstr_t caller() const					= 0;
	virtual void result(const std::map<xstr_t, xstr_t>& values) 	= 0;
};
typedef XPtr<RGroupMgetCallback> RGroupMgetCallbackPtr;


class RedisGroup: virtual public XRefCount, public XEvent::TaskHandler
{
public:
	RedisGroup(const XEvent::DispatcherPtr& dispatcher, const std::string& service, const std::string& servers);
	virtual ~RedisGroup();

	void shutdown();

	// NB: the callback must own the value.
	void _1call(const RedisResultCallbackPtr& cb, const xstr_t& key, const vbs_list_t *cmd);
	void _ncall(const RedisResultCallbackPtr& cb, const xstr_t& key, const vbs_list_t *cmds);
	void _tcall(const RedisResultCallbackPtr& cb, const xstr_t& key, const vbs_list_t *cmds);
	void _ncall(const RedisResultCallbackPtr& cb, const xstr_t& key, const std::vector<std::vector<const vbs_data_t*> >& cmds);
	void _tcall(const RedisResultCallbackPtr& cb, const xstr_t& key, const std::vector<std::vector<const vbs_data_t*> >& cmds);

	void set(const RedisResultCallbackPtr& cb, const xstr_t& key, const xstr_t& value, int expire);
	void remove(const RedisResultCallbackPtr& cb, const xstr_t& key);
	void increment(const RedisResultCallbackPtr& cb, const xstr_t& key, int64_t value);
	void decrement(const RedisResultCallbackPtr& cb, const xstr_t& key, int64_t value);

	void get(const RedisResultCallbackPtr& cb, const xstr_t& key);
	void getMulti(const RGroupMgetCallbackPtr& cb, const std::vector<xstr_t>& keys);

	std::string whichServer(const xstr_t& key, std::string& canonical);
	void allServers(std::vector<std::string>& all, std::vector<std::string>& bad);

	virtual void event_on_task(const XEvent::DispatcherPtr& dispatcher);

private:
	void doit(const RedisOperationPtr& op, const xstr_t& key);
	RedisClientPtr appoint(const xstr_t& key);

private:
	XEvent::DispatcherPtr _dispatcher;
	std::string _service;
	std::vector<RedisClientPtr> _clients;
	UniquePtr<HSequence> _hseq;
	bool _shutdown;
};


#endif
