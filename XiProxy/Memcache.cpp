#include "Memcache.h"
#include "dlog/dlog.h"
#include "xslib/rdtsc.h"
#include "xslib/xatomic.h"
#include "xslib/XError.h"
#include <stdint.h>
#include <map>
#include <vector>

#define CHECK_INTERVAL	(29*1000)
#define HASH_MASK	((1<<16) - 1)

Memcache::Memcache(const XEvent::DispatcherPtr& dispatcher, const std::string& service, const std::string& servers)
	: _dispatcher(dispatcher), _service(service)
{
	xstr_t xs = XSTR_CXX(servers);
	xstr_t item;

	std::vector<xstr_t> items;
	while (xstr_token_space(&xs, &item))
	{
		std::string server = make_string(item);
		MClientPtr client(new MClient(_dispatcher, _service, server, 0));
		client->start();
		_clients.push_back(client);

		items.push_back(item);
	}

	_hseq.reset(new HSequence(items, HASH_MASK));
	_hseq->enable_cache();
	_shutdown = false;
}

Memcache::~Memcache()
{
	shutdown();
}

void Memcache::shutdown()
{
	_shutdown = true;
	for (size_t i = 0; i < _clients.size(); ++i)
	{
		_clients[i]->shutdown();
	}
	_clients.clear();
}


void Memcache::set(const MCallbackPtr& cb, const xstr_t& key, const xstr_t& value, int expire, uint32_t flag)
{
	MOperationPtr op(new MO_set(cb, key, value, expire, flag));
	doit(op, key);
}

void Memcache::replace(const MCallbackPtr& cb, const xstr_t& key, const xstr_t& value, int expire, uint32_t flag)
{
	MOperationPtr op(new MO_replace(cb, key, value, expire, flag));
	doit(op, key);
}

void Memcache::add(const MCallbackPtr& cb, const xstr_t& key, const xstr_t& value, int expire, uint32_t flag)
{
	MOperationPtr op(new MO_add(cb, key, value, expire, flag));
	doit(op, key);
}

void Memcache::cas(const MCallbackPtr& cb, const xstr_t& key, const xstr_t& value, int64_t revision, int expire, uint32_t flag)
{
	MOperationPtr op(new MO_cas(cb, key, value, revision, expire, flag));
	doit(op, key);
}

void Memcache::append(const MCallbackPtr& cb, const xstr_t& key, const xstr_t& value)
{
	MOperationPtr op(new MO_append(cb, key, value));
	doit(op, key);
}

void Memcache::prepend(const MCallbackPtr& cb, const xstr_t& key, const xstr_t& value)
{
	MOperationPtr op(new MO_prepend(cb, key, value));
	doit(op, key);
}

void Memcache::remove(const MCallbackPtr& cb, const xstr_t& key)
{
	MOperationPtr op(new MO_remove(cb, key));
	doit(op, key);
}

void Memcache::increment(const MCallbackPtr& cb, const xstr_t& key, int64_t value)
{
	if (value < 0)
	{
		dlog("MC_WARNING", "memcache can't increment negative number, key=%.*s value=%jd", XSTR_P(&key), (intmax_t)value);
		cb->completed(false);
	}
	else
	{
		MOperationPtr op(new MO_increment(cb, key, value));
		doit(op, key);
	}
}

void Memcache::decrement(const MCallbackPtr& cb, const xstr_t& key, int64_t value)
{
	if (value < 0)
	{
		dlog("MC_WARNING", "memcache can't decrement negative number, key=%.*s value=%jd", XSTR_P(&key), (intmax_t)value);
		cb->completed(false);
	}
	else
	{
		MOperationPtr op(new MO_decrement(cb, key, value));
		doit(op, key);
	}
}

void Memcache::get(const MCallbackPtr& cb, const xstr_t& key)
{
	MOperationPtr op(new MO_get(cb, key));
	doit(op, key);
}

class GetMultiCallback: public MCallback, private XMutex
{
	MCallbackPtr _callback;
	size_t _total;
	size_t _over;
public:
	GetMultiCallback(const MCallbackPtr& cb, size_t total)
		: MCallback(MOC_GETMULTI), _callback(cb), _total(total), _over(0)
	{
	}

	virtual xstr_t caller() const
	{
		return _callback->caller();
	}

	virtual void received(int64_t value)
	{
		throw XERROR_MSG(XLogicError, "Can't reach here");
	}
	
	virtual void received(const MValue values[], size_t num, bool cache, void (*cleanup)(void *), void *cleanup_arg)
	{
		Lock lock(*this);
		_callback->received(values, num, cache, cleanup, cleanup_arg);
	}

	virtual void completed(bool ok, bool zip)
	{
		Lock lock(*this);
		++_over;
		if (_over == _total)
			_callback->completed(true, zip);
		else if (_over > _total)
			throw XERROR_MSG(XLogicError, "Can't reach here");
	}
};
typedef XPtr<GetMultiCallback> GetMultiCallbackPtr;

void Memcache::getMulti(const MCallbackPtr& cb, const std::vector<xstr_t>& keys)
{
	size_t size = keys.size();
	std::map<MClientPtr, std::vector<xstr_t> > ck;
	for (size_t i = 0; i < size; ++i)
	{
		const xstr_t& key = keys[i];
		MClientPtr client = appoint(key);
		if (client)
		{
			std::map<MClientPtr, std::vector<xstr_t> >::iterator iter = ck.find(client);
			if (iter == ck.end())
			{
				iter = ck.insert(std::make_pair(client, std::vector<xstr_t>())).first;
			}
			iter->second.push_back(key);
		}
	}

	if (ck.size())
	{
		GetMultiCallbackPtr callback(new GetMultiCallback(cb, ck.size()));
		for (std::map<MClientPtr, std::vector<xstr_t> >::iterator iter = ck.begin(); iter != ck.end(); ++iter)
		{
			const MClientPtr& client = iter->first;
			MOperationPtr op(new MO_getMulti(callback, iter->second));
			client->process(op);
		}
	}
	else
	{
		cb->completed(true);
	}
}

std::string Memcache::whichServer(const xstr_t& key, std::string& canonical)
{
	std::string real;
	int x = _hseq->which(key.data, key.len);
	if (x >= 0)
	{
		canonical = _clients[x]->server();
		MClientPtr client = appoint(key);
		if (client)
			real = client->server();
	}
	return real;
}

void Memcache::allServers(std::vector<std::string>& all, std::vector<std::string>& bad)
{
	size_t size = _clients.size();
	for (size_t i = 0; i < size; ++i)
	{
		MClientPtr& client = _clients[i];
		all.push_back(client->server());
		if (client->error())
			bad.push_back(client->server());
	}
}

void Memcache::doit(const MOperationPtr& op, const xstr_t& key)
{
	MClientPtr client = appoint(key);
	if (client)
	{
		client->process(op);
	}
	else
	{
		dlog("MC_WARNING", "No healthy memcached server for key=%.*s", XSTR_P(&key));
		op->callback()->completed(false);
	}
}

MClientPtr Memcache::appoint(const xstr_t& key)
{
	MClientPtr client;
	int x = _hseq->which(key.data, key.len);
	if (x >= 0)
	{
		if (!_clients[x]->error())
		{
			client = _clients[x];
		}
		else
		{
			int seqs[5];
			int n = _hseq->sequence(key.data, key.len, seqs, 5);
			for (int i = 1; i < n; ++i)
			{
				x = seqs[i];
				if (!_clients[x]->error())
				{
					client = _clients[x];
					break;
				}
			}
		}
	}

	return client;
}

