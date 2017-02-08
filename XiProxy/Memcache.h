#ifndef Memcache_h_
#define Memcache_h_

#include "xslib/XRefCount.h"
#include "xslib/xstr.h"
#include "xslib/HSequence.h"
#include "MClient.h"
#include <string>
#include <vector>
#include <memory>

class Memcache;
typedef XPtr<Memcache> MemcachePtr;


class Memcache: virtual public XRefCount
{
public:
	Memcache(const XEvent::DispatcherPtr& dispatcher, const std::string& service, const std::string& servers);
	virtual ~Memcache();

	void shutdown();

	// NB: the callback must own the value.
	void set(const MCallbackPtr& cb, const xstr_t& key, const xstr_t& value, int expire, uint32_t flag);
	void replace(const MCallbackPtr& cb, const xstr_t& key, const xstr_t& value, int expire, uint32_t flag);
	void add(const MCallbackPtr& cb, const xstr_t& key, const xstr_t& value, int expire, uint32_t flag);
	void cas(const MCallbackPtr& cb, const xstr_t& key, const xstr_t& value, int64_t revision, int expire, uint32_t flag);

	void append(const MCallbackPtr& cb, const xstr_t& key, const xstr_t& value);
	void prepend(const MCallbackPtr& cb, const xstr_t& key, const xstr_t& value);

	void remove(const MCallbackPtr& cb, const xstr_t& key);
	void increment(const MCallbackPtr& cb, const xstr_t& key, int64_t value);
	void decrement(const MCallbackPtr& cb, const xstr_t& key, int64_t value);

	void get(const MCallbackPtr& cb, const xstr_t& key);
	void getMulti(const MCallbackPtr& cb, const std::vector<xstr_t>& keys);

	std::string whichServer(const xstr_t& key, std::string& canonical);
	void allServers(std::vector<std::string>& all, std::vector<std::string>& bad);

private:
	void doit(const MOperationPtr& op, const xstr_t& key);
	MClientPtr appoint(const xstr_t& key);

private:
	XEvent::DispatcherPtr _dispatcher;
	std::string _service;
	std::vector<MClientPtr> _clients;
	std::auto_ptr<HSequence> _hseq;
	bool _shutdown;
};


#endif
