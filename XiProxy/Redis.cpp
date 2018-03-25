#include "Redis.h"
#include "xic/Engine.h"
#include "dlog/dlog.h"
#include "xslib/vbs.h"
#include "xslib/xlog.h"


xic::MethodTab::PairType Redis::_funpairs[] = {
#define CMD(X)	{ #X, XIC_METHOD_CAST(Redis, X) },
	REDIS_CMDS
#undef CMD
};

xic::MethodTab Redis::_funtab(_funpairs, XS_ARRCOUNT(_funpairs));

static pthread_once_t dispatcher_once = PTHREAD_ONCE_INIT;
static XEvent::DispatcherPtr the_dispatcher;

static void start_dispatcher()
{
	the_dispatcher = XEvent::Dispatcher::create();
	the_dispatcher->setThreadPool(4, 4, 1024*256);
	the_dispatcher->start();
}

Redis::Redis(const xic::EnginePtr& engine, const std::string& service, int revision, const std::string& servers)
	: RevServant(engine, service, revision), _servers(servers)
{
	pthread_once(&dispatcher_once, start_dispatcher);

	_redisgroup.reset(new RedisGroup(the_dispatcher, _service, servers));
}

Redis::~Redis()
{
	_redisgroup->shutdown();
	_redisgroup.reset();
}

xic::AnswerPtr Redis::process(const xic::QuestPtr& quest, const xic::Current& current)
try {
	return xic::process_servant_method(this, &_funtab, quest, current);
}
catch (std::exception& ex)
{
	xic::Quest* q = quest.get();
	const xstr_t& service = q->service();
	const xstr_t& method = q->method();
	edlog(vbs_xfmt, "RDS_WARNING", "Q=%.*s::%.*s C%p{>VBS_DICT<} exception=%s",
		XSTR_P(&service), XSTR_P(&method), q->context_dict(), ex.what());
	throw; 
}

void Redis::getInfo(xic::VDictWriter& dw)
{
	RevServant::getInfo(dw);
	dw.kv("type", "internal");
	dw.kv("servers", _servers);
}

class Callback_default: public RedisResultCallback
{
	xic::WaiterPtr _waiter;

public:
	Callback_default(const xic::WaiterPtr& waiter)
		: _waiter(waiter)
	{
	}

	~Callback_default()
	{
	}

	virtual xstr_t caller() const
	{
		const xic::QuestPtr& q = _waiter->quest();
		return q->context().getXstr("CALLER");
	}

	virtual bool completed(const vbs_list_t& ls)
	{
		try {
			xic::AnswerWriter aw;
			handle(aw, ls);
			_waiter->response(aw);
		}
		catch (std::exception& ex)
		{
			_waiter->response(ex);
		}
		return true;
	}

	virtual void exception(const std::exception& ex)
	{
		_waiter->response(ex);
	}

	virtual void handle(xic::AnswerWriter& aw, const vbs_list_t& ls) = 0;
};

struct Callback_1call: public Callback_default
{
	Callback_1call(const xic::WaiterPtr& waiter)
		: Callback_default(waiter)
	{
	}

	virtual void handle(xic::AnswerWriter& aw, const vbs_list_t& ls)
	{
		const vbs_data_t *d0 = ls.first ? &ls.first->value : NULL;
		if (d0)
			aw.param("result", d0);
		else
			aw.paramNull("result");
	}
};

struct Callback_ncall: public Callback_default
{
	size_t _num;

	Callback_ncall(const xic::WaiterPtr& waiter, size_t num)
		: Callback_default(waiter), _num(num)
	{
	}

	virtual void handle(xic::AnswerWriter& aw, const vbs_list_t& ls)
	{
		xic::VListWriter lw = aw.paramVList("results");
		size_t n = 0;
		for (vbs_litem_t *ent = ls.first; ent && n < _num; ent = ent->next, ++n)
		{
			lw.v(ent->value);
		}

		for (; n < _num; ++n)
		{
			lw.vnull();
		}
	}
};

struct Callback_tcall: public Callback_default
{
	size_t _num;

	Callback_tcall(const xic::WaiterPtr& waiter, size_t num)
		: Callback_default(waiter), _num(num)
	{
	}

	virtual void handle(xic::AnswerWriter& aw, const vbs_list_t& ls)
	{
		if (ls.count < _num + 2)
			throw XERROR_MSG(XError, "number of replies less than that of requests");

		size_t n = 0;
		vbs_list_t *result = NULL;
		for (vbs_litem_t *ent = ls.first; ent; ent = ent->next, ++n)
		{
			vbs_data_t *d = &ent->value;
			if (n == 0)
			{
				if (d->kind != VBS_STRING || !xstr_equal_cstr(&d->d_xstr, "+OK"))
					throw XERROR_MSG(XError, "invalid reply for MULTI cmd");
			}
			else if (n == _num + 1)
			{
				if (d->kind != VBS_LIST)
					throw XERROR_MSG(XError, "invalid reply for EXEC cmd");

				result = d->d_list;
				break;
			}
		}

		xic::VListWriter lw = aw.paramVList("results");
		n = 0;
		vbs_litem_t *result_ent = result->first;
		for (vbs_litem_t *ent = ls.first->next; ent; ent = ent->next, ++n)
		{
			if (n >= _num)
				break;

			const vbs_data_t *d = &ent->value;
			if (d->kind != VBS_STRING || !xstr_equal_cstr(&d->d_xstr, "+QUEUED"))
			{
				lw.v(d);
			}
			else
			{
				if (result_ent)
				{
					lw.v(result_ent->value);
					result_ent = result_ent->next;
				}
				else
				{
					lw.vnull();
				}
			}
		}
	}
};

struct Callback_set: public Callback_default
{
	Callback_set(const xic::WaiterPtr& waiter)
		: Callback_default(waiter)
	{
	}

	virtual void handle(xic::AnswerWriter& aw, const vbs_list_t& ls)
	{
		vbs_data_t *d0 = ls.first ? &ls.first->value : NULL;
		if (d0 && d0->kind == VBS_STRING && xstr_equal_cstr(&d0->d_xstr, "+OK"))
			aw.param("ok", d0->d_bool);
		else
			aw.param("ok", false);
	}
};

struct Callback_delete: public Callback_default
{
	Callback_delete(const xic::WaiterPtr& waiter)
		: Callback_default(waiter)
	{
	}

	virtual void handle(xic::AnswerWriter& aw, const vbs_list_t& ls)
	{
		vbs_data_t *d0 = ls.first ? &ls.first->value : NULL;
		if (d0 && d0->kind == VBS_INTEGER && d0->d_int > 0)
			aw.param("ok", true);
		else
			aw.param("ok", false);
	}
};

struct Callback_inc_dec: public Callback_default
{
	Callback_inc_dec(const xic::WaiterPtr& waiter)
		: Callback_default(waiter)
	{
	}

	virtual void handle(xic::AnswerWriter& aw, const vbs_list_t& ls)
	{
		vbs_data_t *d0 = ls.first ? &ls.first->value : NULL;
		if (d0 && d0->kind == VBS_INTEGER)
		{
			aw.param("ok", true);
			aw.param("value", d0->d_int);
		}
		else
			aw.param("ok", false);
	}
};

struct Callback_get: public Callback_default
{
	Callback_get(const xic::WaiterPtr& waiter)
		: Callback_default(waiter)
	{
	}

	virtual void handle(xic::AnswerWriter& aw, const vbs_list_t& ls)
	{
		const vbs_data_t *d0 = ls.first ? &ls.first->value : NULL;
		if (d0 && d0->kind == VBS_BLOB)
		{
			aw.param("value", d0);
		}
	}
};

class Callback_getMulti: public RGroupMgetCallback
{
	xic::WaiterPtr _waiter;
public:
	Callback_getMulti(const xic::WaiterPtr& waiter)
		: _waiter(waiter)
	{
	}

	virtual xstr_t caller() const
	{
		const xic::QuestPtr& q = _waiter->quest();
		return q->context().getXstr("CALLER");
	}

	virtual void result(const std::map<xstr_t, xstr_t>& values)
	{
		xic::AnswerWriter aw;
		xic::VDictWriter dw = aw.paramVDict("values");
		for (std::map<xstr_t, xstr_t>::const_iterator iter = values.begin(); iter != values.end(); ++iter)
		{
			const xstr_t& k = iter->first;
			const xstr_t& v = iter->second;
			dw.kvblob(k, v);
		}
		_waiter->response(aw);
	}
};


void check_cmd(ostk_t *ostk, const xic::VList& cmd, std::vector<const vbs_data_t*>& rcmd)
{
	if (cmd.size() < 1)
		throw XERROR_MSG(XError, "empty cmd");

	xic::VList::Node node = cmd.first();
	if (node.valueType() != VBS_STRING)
		throw XERROR_MSG(XError, "invalid cmd name");

	rcmd.reserve(cmd.size());
	rcmd.push_back(node.dataValue());
	for (++node; node; ++node)
	{
		switch (node.valueType())
		{
		case VBS_STRING:
		case VBS_BLOB:
		case VBS_INTEGER:
		case VBS_FLOATING:
		case VBS_BOOL:
			rcmd.push_back(node.dataValue());
			break;
		default:
			throw XERROR_MSG(XError, "invalid cmd args");
		}
	}
}

XIC_METHOD(Redis, _1CALL)
{
	xic::VDict args = quest->args();
	const xstr_t& key = args.wantXstr("key");
	const vbs_list_t* cmd = args.want_list("cmd");

	RedisResultCallbackPtr cb(new Callback_1call(current.asynchronous()));
	_redisgroup->_1call(cb, key, cmd);
	return xic::ASYNC_ANSWER;
}

XIC_METHOD(Redis, _NCALL)
{
	xic::VDict args = quest->args();
	const xstr_t& key = args.wantXstr("key");
	const vbs_list_t* cmds = args.want_list("cmds");

	RedisResultCallbackPtr cb(new Callback_ncall(current.asynchronous(), cmds->count));
	_redisgroup->_ncall(cb, key, cmds);
	return xic::ASYNC_ANSWER;
}

XIC_METHOD(Redis, _TCALL)
{
	xic::VDict args = quest->args();
	const xstr_t& key = args.wantXstr("key");
	const vbs_list_t* cmds = args.want_list("cmds");

	RedisResultCallbackPtr cb(new Callback_tcall(current.asynchronous(), cmds->count));
	_redisgroup->_tcall(cb, key, cmds);
	return xic::ASYNC_ANSWER;
}

XIC_METHOD(Redis, set)
{
	xic::VDict args = quest->args();
	const xstr_t& key = args.wantXstr("key");
	const xstr_t& value = args.wantBlob("value");
	int expire = args.getInt("expire");

	RedisResultCallbackPtr cb(new Callback_set(current.asynchronous()));
	_redisgroup->set(cb, key, value, expire);
	return xic::ASYNC_ANSWER;
}

XIC_METHOD(Redis, delete)
{
	xic::VDict args = quest->args();
	const xstr_t& key = args.wantXstr("key");

	RedisResultCallbackPtr cb(new Callback_delete(current.asynchronous()));
	_redisgroup->remove(cb, key);
	return xic::ASYNC_ANSWER;
}

XIC_METHOD(Redis, increment)
{
	xic::VDict args = quest->args();
	const xstr_t& key = args.wantXstr("key");
	int64_t value = args.wantInt("value");

	RedisResultCallbackPtr cb(new Callback_inc_dec(current.asynchronous()));
	_redisgroup->increment(cb, key, value);
	return xic::ASYNC_ANSWER;
}

XIC_METHOD(Redis, decrement)
{
	xic::VDict args = quest->args();
	const xstr_t& key = args.wantXstr("key");
	int64_t value = args.wantInt("value");

	RedisResultCallbackPtr cb(new Callback_inc_dec(current.asynchronous()));
	_redisgroup->decrement(cb, key, value);
	return xic::ASYNC_ANSWER;
}


XIC_METHOD(Redis, get)
{
	xic::VDict args = quest->args();
	const xstr_t& key = args.wantXstr("key");
	
	RedisResultCallbackPtr cb(new Callback_get(current.asynchronous()));
	_redisgroup->get(cb, key);
	return xic::ASYNC_ANSWER;
}

XIC_METHOD(Redis, getMulti)
{
	xic::VDict args = quest->args();
	std::vector<xstr_t> keys;
	args.wantXstrSeq("keys", keys);

	RGroupMgetCallbackPtr cb(new Callback_getMulti(current.asynchronous()));
	_redisgroup->getMulti(cb, keys);
	return xic::ASYNC_ANSWER;
}

XIC_METHOD(Redis, whichServer)
{
	xic::VDict args = quest->args();
	const xstr_t& key = args.wantXstr("key");

	xic::AnswerWriter aw;
	std::string canonical;
	std::string real = _redisgroup->whichServer(key, canonical);
	aw.param("real", real);
	aw.param("canonical", canonical);
	return aw;
}

XIC_METHOD(Redis, allServers)
{
	xic::AnswerWriter aw;
	std::vector<std::string> all, bad;
	_redisgroup->allServers(all, bad);

	xic::VListWriter lw = aw.paramVList("all");
	size_t size = all.size();
	for (size_t i = 0; i < size; ++i)
	{
		lw.v(all[i]);
	}

	lw = aw.paramVList("bad");
	size = bad.size();
	for (size_t i = 0; i < size; ++i)
	{
		lw.v(bad[i]);
	}
	return aw;
}

