#include "RedisGroup.h"
#include "dlog/dlog.h"
#include "xslib/rdtsc.h"
#include "xslib/xatomic.h"
#include "xslib/XError.h"
#include <stdint.h>
#include <map>
#include <vector>

#define CHECK_INTERVAL	(29*1000)
#define HASH_MASK	((1<<16) - 1)

RedisGroup::RedisGroup(const XEvent::DispatcherPtr& dispatcher, const std::string& service, const std::string& servers)
	: _dispatcher(dispatcher), _service(service)
{
	xstr_t xs = XSTR_CXX(servers);
	std::string password;
	if (xstr_find_char(&xs, 0, '^') >= 0)
	{
		xstr_t tmp;
		xstr_key_value(&xs, '^', &tmp, &xs);
		password = make_string(tmp);
	}

	xstr_t item;
	std::vector<xstr_t> items;
	while (xstr_token_space(&xs, &item))
	{
		std::string server = make_string(item);
		RedisClientPtr client(new RedisClient(_dispatcher, _service, server, password, 0));
		_clients.push_back(client);

		items.push_back(item);
	}

	_hseq.reset(new HSequence(items, HASH_MASK));
	_hseq->enable_cache();
	_shutdown = false;

	xref_inc();
	_dispatcher->addTask(this, CHECK_INTERVAL);
	xref_dec_only();
}

RedisGroup::~RedisGroup()
{
	shutdown();
}

void RedisGroup::shutdown()
{
	_shutdown = true;
	for (size_t i = 0; i < _clients.size(); ++i)
	{
		_clients[i]->shutdown();
	}
	_clients.clear();
}

void RedisGroup::_1call(const RedisResultCallbackPtr& cb, const xstr_t& key, const vbs_list_t* cmd)
{
	RedisOperationPtr op(new RO_1call(cb, cmd));
	doit(op, key);
}

void RedisGroup::_ncall(const RedisResultCallbackPtr& cb, const xstr_t& key, const vbs_list_t* cmds)
{
	RedisOperationPtr op(new RO_ncall(cb, cmds));
	doit(op, key);
}

void RedisGroup::_tcall(const RedisResultCallbackPtr& cb, const xstr_t& key, const vbs_list_t* cmds)
{
	RedisOperationPtr op(new RO_tcall(cb, cmds));
	doit(op, key);
}

void RedisGroup::set(const RedisResultCallbackPtr& cb, const xstr_t& key, const xstr_t& value, int expire)
{
	RedisOperationPtr op(new RO_set(cb, key, value, expire));
	doit(op, key);
}

void RedisGroup::remove(const RedisResultCallbackPtr& cb, const xstr_t& key)
{
	RedisOperationPtr op(new RO_remove(cb, key));
	doit(op, key);
}

void RedisGroup::increment(const RedisResultCallbackPtr& cb, const xstr_t& key, int64_t value)
{
	if (value < 0)
	{
		XERROR_VAR_FMT(XError, ex, "can't increment negative number, key=%.*s value=%jd", XSTR_P(&key), (intmax_t)value);
		cb->exception(ex);
		dlog("RDS_WARNING", "%s", ex.what());
	}
	else
	{
		RedisOperationPtr op(new RO_increment(cb, key, value));
		doit(op, key);
	}
}

void RedisGroup::decrement(const RedisResultCallbackPtr& cb, const xstr_t& key, int64_t value)
{
	if (value < 0)
	{
		XERROR_VAR_FMT(XError, ex, "can't decrement negative number, key=%.*s value=%jd", XSTR_P(&key), (intmax_t)value);
		cb->exception(ex);
		dlog("RDS_WARNING", "%s", ex.what());
	}
	else
	{
		RedisOperationPtr op(new RO_decrement(cb, key, value));
		doit(op, key);
	}
}

void RedisGroup::get(const RedisResultCallbackPtr& cb, const xstr_t& key)
{
	RedisOperationPtr op(new RO_get(cb, key));
	doit(op, key);
}

class GatherCallback: public XRefCount, private XMutex
{
	RGroupMgetCallbackPtr _callback;
	std::map<xstr_t, xstr_t> _values;
	size_t _total;
	size_t _over;
public:
	GatherCallback(const RGroupMgetCallbackPtr& cb, size_t total)
		: _callback(cb), _total(total), _over(0)
	{
	}

	virtual xstr_t caller() const
	{
		return _callback->caller();
	}

	void values(const std::vector<xstr_t>& keys, const vbs_list_t& ls)
	{
		Lock lock(*this);
		size_t i = 0;
		size_t num = keys.size();
		for (vbs_litem_t *ent = ls.first; ent && i < num; ++i, ent = ent->next)
		{
			vbs_data_t* d = &ent->value;
			if (d->type == VBS_BLOB)
			{
				_values.insert(std::make_pair(keys[i], d->d_blob));
			}
		}

		++_over;
		if (_over == _total)
		{
			_callback->result(_values);
		}
		else if (_over > _total)
			throw XERROR_MSG(XLogicError, "Can't reach here");
	}
};
typedef XPtr<GatherCallback> GatherCallbackPtr;

class Callback_mget: public RedisResultCallback
{
	GatherCallbackPtr _callback;
	std::vector<xstr_t> _keys;

	void _empty_result()
	{
		vbs_list_t ls;
		vbs_list_init(&ls, 0);
		_callback->values(_keys, ls);
	}

public:
	Callback_mget(const GatherCallbackPtr& cb, const std::vector<xstr_t>& keys)
		: _callback(cb), _keys(keys)
	{
	}

	virtual xstr_t caller() const
	{
		return _callback->caller();
	}

	virtual bool completed(const vbs_list_t& ls)
	{
		if (ls.first && ls.first->value.type == VBS_LIST)
			_callback->values(_keys, *ls.first->value.d_list);
		else
			_empty_result();
		return true;
	}

	virtual void exception(const std::exception& ex)
	{
		_empty_result();
	}
};

void RedisGroup::getMulti(const RGroupMgetCallbackPtr& cb, const std::vector<xstr_t>& keys)
{
	size_t size = keys.size();
	std::map<RedisClientPtr, std::vector<xstr_t> > ck;
	for (size_t i = 0; i < size; ++i)
	{
		const xstr_t& key = keys[i];
		RedisClientPtr client = appoint(key);
		if (client)
		{
			std::map<RedisClientPtr, std::vector<xstr_t> >::iterator iter = ck.find(client);
			if (iter == ck.end())
			{
				iter = ck.insert(std::make_pair(client, std::vector<xstr_t>())).first;
			}
			iter->second.push_back(key);
		}
	}

	if (ck.size())
	{
		GatherCallbackPtr gather(new GatherCallback(cb, ck.size()));
		for (std::map<RedisClientPtr, std::vector<xstr_t> >::iterator iter = ck.begin(); iter != ck.end(); ++iter)
		{
			const RedisClientPtr& client = iter->first;
			const std::vector<xstr_t>& keys = iter->second;
			RedisResultCallbackPtr mget_cb(new Callback_mget(gather, keys));
			RedisOperationPtr op(new RO_mget(mget_cb, keys));
			client->process(op);
		}
	}
	else
	{
		cb->result(std::map<xstr_t, xstr_t>());
	}
}

std::string RedisGroup::whichServer(const xstr_t& key, std::string& canonical)
{
	std::string real;
	int x = _hseq->which(key.data, key.len);
	if (x >= 0)
	{
		canonical = _clients[x]->server();
		RedisClientPtr client = appoint(key);
		if (client)
			real = client->server();
	}
	return real;
}

void RedisGroup::allServers(std::vector<std::string>& all, std::vector<std::string>& bad)
{
	size_t size = _clients.size();
	for (size_t i = 0; i < size; ++i)
	{
		RedisClientPtr& client = _clients[i];
		all.push_back(client->server());
		if (client->error())
			bad.push_back(client->server());
	}
}

void RedisGroup::doit(const RedisOperationPtr& op, const xstr_t& key)
{
	RedisClientPtr client = appoint(key);
	if (client)
	{
		client->process(op);
	}
	else
	{
		XERROR_VAR_MSG(XError, ex, "No available server");
		op->finish(NULL, ex);
	}
}

RedisClientPtr RedisGroup::appoint(const xstr_t& key)
{
	RedisClientPtr client;
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

void RedisGroup::event_on_task(const XEvent::DispatcherPtr& dispatcher)
{
	for (size_t i = 0; i < _clients.size(); ++i)
	{
		if (_clients[i]->error())
		{
			dlog("RDS_ALERT", "server=%s", _clients[i]->server().c_str());
		}
	}

	if (!_shutdown)
		dispatcher->addTask(this, CHECK_INTERVAL);
}

