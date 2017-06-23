#include "MCache.h"
#include "lz4codec.h"
#include "xic/Engine.h"
#include "dlog/dlog.h"
#include "xslib/vbs.h"
#include "xslib/xlog.h"
#include "xslib/rdtsc.h"
#include "xslib/cxxstr.h"


xic::MethodTab::PairType MCache::_funpairs[] = {
#define CMD(X)	{ #X, XIC_METHOD_CAST(MCache, X) },
	MCACHE_CMDS
#undef CMD
};

xic::MethodTab MCache::_funtab(_funpairs, XS_ARRCOUNT(_funpairs));

static pthread_once_t dispatcher_once = PTHREAD_ONCE_INIT;
static XEvent::DispatcherPtr the_dispatcher;

static void start_dispatcher()
{
	the_dispatcher = XEvent::Dispatcher::create();
	the_dispatcher->setThreadPool(4, 4, 1024*256);
	the_dispatcher->start();
}

MCache::MCache(const xic::EnginePtr& engine, const std::string& service, int revision, const std::string& servers, const RCachePtr& rcache)
	: RevServant(engine, service, revision), _servers(servers), _rcache(rcache)
{
	pthread_once(&dispatcher_once, start_dispatcher);

	_memcache.reset(new Memcache(the_dispatcher, _service, servers));
}

MCache::~MCache()
{
	_memcache->shutdown();
	_memcache.reset();
}

xic::AnswerPtr MCache::process(const xic::QuestPtr& quest, const xic::Current& current)
try {
	return xic::process_servant_method(this, &_funtab, quest, current);
}
catch (std::exception& ex)
{
	xic::Quest* q = quest.get();
	const xstr_t& service = q->service();
	const xstr_t& method = q->method();
	edlog(vbs_xfmt, "MC_WARNING", "Q=%.*s::%.*s C%p{>VBS_DICT<} exception=%s",
		XSTR_P(&service), XSTR_P(&method), q->context_dict(), ex.what());
	throw; 
}

void MCache::getInfo(xic::VDictWriter& dw)
{
	RevServant::getInfo(dw);
	dw.kv("type", "internal");
	dw.kv("servers", _servers);
}

class MCacheCallback: public MCallback
{
	xic::WaiterPtr _waiter;
	RCachePtr _rcache;
	std::string _service;
	xic::AnswerWriter _aw;
	int64_t _ivalue;
	std::vector<MValue> _mvalues;
public:
	MCacheCallback(MOCategory category, const xic::WaiterPtr& waiter)
		: MCallback(category), _waiter(waiter)
	{
		_ivalue = 0;
	}

	MCacheCallback(MOCategory category, const xic::WaiterPtr& waiter, const RCachePtr& rcache, const std::string& service)
		: MCallback(category), _waiter(waiter), _rcache(rcache), _service(service)
	{
		_ivalue = 0;
	}

	virtual xstr_t caller() const;
	virtual void received(int64_t value);
	virtual void received(const MValue vals[], size_t num, bool cache, void (*cleanup)(void *), void *cleanup_arg);
	virtual void completed(bool ok, bool zip = false);
};

xstr_t MCacheCallback::caller() const
{
	const xic::QuestPtr& q = _waiter->quest();
	return q->context().getXstr("CALLER");
}

void MCacheCallback::received(int64_t value)
{
	_ivalue = value;
}

void MCacheCallback::received(const MValue vals[], size_t num, bool cache, void (*cleanup)(void *), void *cleanup_arg)
{
	if (cleanup)
		_aw.cleanup_push(cleanup, cleanup_arg);

	if (num)
	{
		xstr_t service = XSTR_CXX(_service);
		ostk_t *ostk = _aw.ostk();
		for (size_t i = 0; i < num; ++i)
		{
			MValue mv = vals[i];
			if (mv.flags & FLAG_LZ4_ZIP)
			{
				int rc = attempt_lz4_unzip(ostk, mv.value, mv.value);
				if (rc < 0)
				{
					dlog("MC_UNZIP", "attempt_lz4_unzip()=%d service=%.*s key=%.*s flag=%#x",
							rc, XSTR_P(&service), XSTR_P(&mv.key), mv.flags);
					continue;
				}
				else
				{
					mv.flags &= ~FLAG_LZ4_ZIP;
				}
			}
			else if (!cleanup)
			{
				mv.key = ostk_xstr_dup(ostk, &mv.key);
				mv.value = ostk_xstr_dup(ostk, &mv.value);
			}

			_mvalues.push_back(mv);

			if (cache && _rcache)
			{
				RKey rkey(service, mv.key);
				RData rdata(rdtsc(), RD_MCACHE, mv.value);
				_rcache->replace(rkey, rdata);
			}
		}
	}
}

void MCacheCallback::completed(bool ok, bool zip)
{
	if (_category == MOC_COUNT)
	{
		_aw.param("ok", ok);
		if (ok)
			_aw.param("value", _ivalue);
	}
	else if (_category == MOC_GET)
	{
		if (ok && _mvalues.size())
		{
			_aw.paramBlob("value", _mvalues[0].value);
			_aw.param("revision", _mvalues[0].revision);
			_aw.param("_zip", _mvalues[0].zip);
		}
	}
	else if (_category == MOC_GETMULTI)
	{
		size_t size = _mvalues.size();
		xic::VDictWriter dw = _aw.paramVDict("values");
		if (ok)
		{
			for (size_t i = 0; i < size; ++i)
			{
				dw.kvblob(_mvalues[i].key, _mvalues[i].value);
			}
		}

		dw = _aw.paramVDict("revisions");
		if (ok)
		{
			for (size_t i = 0; i < size; ++i)
			{
				dw.kv(_mvalues[i].key, _mvalues[i].revision);
			}
		}
	}
	else
	{
		_aw.param("ok", ok);
		if (_category == MOC_STORE || _category == MOC_CAS)
			_aw.param("_zip", zip);
	}
	_waiter->response(_aw.take());
}

/*
   NB: The key may be changed.
 */
static void prepare_key(const xic::QuestPtr& quest, xstr_t& key, const xstr_t& value = xstr_null)
{
	static xstr_t search = XSTR_CONST(" \t\v\f\r\n\0");
	static xstr_t replace = XSTR_CONST("\x1f\x1e\x1d\x1c\x1a");
	static bset_t invalid = make_bset_by_add_xstr(&empty_bset, &search);

	if (key.len == 0 || key.len > 250)
		throw XERROR_MSG(XError, xformat_string(vbs_xfmt, "key for memcache can't be empty or larger than 250 bytes, key=%p{>VBS_STRING<} value=%p{>VBS_STRING<}", &key, &value));

	if (value.len >= 1024*1024 - 256)
	{
		xic::Quest* q = quest.get();
		edlog(vbs_xfmt, "MC_WARNING", "Q=%.*s::%.*s C%p{>VBS_DICT<}, value too large (len=%zd), key=%p{>VBS_STRING<}",
			XSTR_P(&q->service()), XSTR_P(&q->method()), q->context_dict(), value.len, &key);
	}

	if (xstr_find_in_bset(&key, 0, &invalid) >= 0)
	{
		xic::Quest* q = quest.get();
		edlog(vbs_xfmt, "MC_NOTICE", "Q=%.*s::%.*s C%p{>VBS_DICT<}, key containing whitespace, key=%p{>VBS_STRING<}",
			XSTR_P(&q->service()), XSTR_P(&q->method()), q->context_dict(), &key);

		xstr_t xs = ostk_xstr_dup(quest->ostk(), &key);
		if (xs.data)
		{
			xstr_replace_in(&xs, &search, &replace);
			key = xs;
		}
	}
}

XIC_METHOD(MCache, set)	
{
	xic::VDict args = quest->args();
	xstr_t key = args.wantXstr("key");
	const xstr_t& value = args.wantBlob("value");
	prepare_key(quest, key, value);
	int expire = args.getInt("expire");
	bool nozip = args.getBool("nozip");
	uint32_t flags = (!nozip && value.len > ZIP_THRESHOLD) ? FLAG_LZ4_ZIP : 0;

	int cache = quest->context().getInt("CACHE");
	RKey rkey(quest->service(), key);
	if (cache)
	{
		RData rdata(rdtsc(), RD_MCACHE, value);
		_rcache->replace(rkey, rdata);
	}
	else
	{
		_rcache->remove(rkey);
	}

	MCallbackPtr cb(new MCacheCallback(MOC_STORE, current.asynchronous()));
	_memcache->set(cb, key, value, expire, flags);
	return xic::ASYNC_ANSWER;
}

XIC_METHOD(MCache, replace)
{
	xic::VDict args = quest->args();
	xstr_t key = args.wantXstr("key");
	const xstr_t& value = args.wantBlob("value");
	prepare_key(quest, key, value);
	int expire = args.getInt("expire");
	bool nozip = args.getBool("nozip");
	uint32_t flags = (!nozip && value.len > ZIP_THRESHOLD) ? FLAG_LZ4_ZIP : 0;

	RKey rkey(quest->service(), key);
	_rcache->remove(rkey);

	MCallbackPtr cb(new MCacheCallback(MOC_STORE, current.asynchronous()));
	_memcache->replace(cb, key, value, expire, flags);
	return xic::ASYNC_ANSWER;
}

XIC_METHOD(MCache, add)
{
	xic::VDict args = quest->args();
	xstr_t key = args.wantXstr("key");
	const xstr_t& value = args.wantBlob("value");
	prepare_key(quest, key, value);
	int expire = args.getInt("expire");
	bool nozip = args.getBool("nozip");
	uint32_t flags = (!nozip && value.len > ZIP_THRESHOLD) ? FLAG_LZ4_ZIP : 0;

	RKey rkey(quest->service(), key);
	_rcache->remove(rkey);

	MCallbackPtr cb(new MCacheCallback(MOC_STORE, current.asynchronous()));
	_memcache->add(cb, key, value, expire, flags);
	return xic::ASYNC_ANSWER;
}

XIC_METHOD(MCache, append)
{
	xic::VDict args = quest->args();
	xstr_t key = args.wantXstr("key");
	const xstr_t& value = args.wantBlob("value");
	prepare_key(quest, key, value);

	RKey rkey(quest->service(), key);
	_rcache->remove(rkey);

	MCallbackPtr cb(new MCacheCallback(MOC_STORE, current.asynchronous()));
	_memcache->append(cb, key, value);
	return xic::ASYNC_ANSWER;
}

XIC_METHOD(MCache, prepend)
{
	xic::VDict args = quest->args();
	xstr_t key = args.wantXstr("key");
	const xstr_t& value = args.wantBlob("value");
	prepare_key(quest, key, value);

	RKey rkey(quest->service(), key);
	_rcache->remove(rkey);

	MCallbackPtr cb(new MCacheCallback(MOC_STORE, current.asynchronous()));
	_memcache->prepend(cb, key, value);
	return xic::ASYNC_ANSWER;
}

XIC_METHOD(MCache, cas)
{
	xic::VDict args = quest->args();
	xstr_t key = args.wantXstr("key");
	const xstr_t& value = args.wantBlob("value");
	prepare_key(quest, key, value);
	int64_t revision = args.wantInt("revision");
	int expire = args.getInt("expire");
	bool nozip = args.getBool("nozip");
	uint32_t flags = (!nozip && value.len > ZIP_THRESHOLD) ? FLAG_LZ4_ZIP : 0;

	RKey rkey(quest->service(), key);
	_rcache->remove(rkey);

	MCallbackPtr cb(new MCacheCallback(MOC_CAS, current.asynchronous()));
	_memcache->cas(cb, key, value, revision, expire, flags);
	return xic::ASYNC_ANSWER;
}

XIC_METHOD(MCache, delete)
{
	xic::VDict args = quest->args();
	xstr_t key = args.wantXstr("key");
	prepare_key(quest, key);

	RKey rkey(quest->service(), key);
	_rcache->remove(rkey);

	MCallbackPtr cb(new MCacheCallback(MOC_DELETE, current.asynchronous()));
	_memcache->remove(cb, key);
	return xic::ASYNC_ANSWER;
}

XIC_METHOD(MCache, increment)
{
	xic::VDict args = quest->args();
	xstr_t key = args.wantXstr("key");
	prepare_key(quest, key);
	int64_t value = args.wantInt("value");

	RKey rkey(quest->service(), key);
	_rcache->remove(rkey);

	MCallbackPtr cb(new MCacheCallback(MOC_COUNT, current.asynchronous()));
	_memcache->increment(cb, key, value);
	return xic::ASYNC_ANSWER;
}

XIC_METHOD(MCache, decrement)
{
	xic::VDict args = quest->args();
	xstr_t key = args.wantXstr("key");
	prepare_key(quest, key);
	int64_t value = args.wantInt("value");

	RKey rkey(quest->service(), key);
	_rcache->remove(rkey);

	MCallbackPtr cb(new MCacheCallback(MOC_COUNT, current.asynchronous()));
	_memcache->decrement(cb, key, value);
	return xic::ASYNC_ANSWER;
}


XIC_METHOD(MCache, get)
{
	xic::VDict args = quest->args();
	xstr_t key = args.wantXstr("key");
	prepare_key(quest, key);
	
	xic::VDict ctx = quest->context();
	int cache = ctx.getInt("CACHE");
	MCallbackPtr cb(new MCacheCallback(MOC_GET, current.asynchronous(), cache ? _rcache : RCachePtr(), make_string(quest->service())));
	if (cache > 0)
	{
		RKey rkey(quest->service(), key);
		RData rdata = _rcache->find(rkey);
		if (rdata && rdata.type() == RD_MCACHE)
		{
			int status = rdata.status();
			// exception answer only cache 1 second.
			uint64_t expire = (status ? 1 : cache) * cpu_frequency();

			if ((rdtsc() - rdata.ctime()) < expire)
			{
				if (status)
				{
					cb->completed(false);
				}
				else
				{
					MValue mv;
					mv.key = key;
					mv.value = rdata.xstr();
					mv.revision = 0;
					mv.flags = 0;
					cb->received(&mv, 1, false, RData::unref_rdata, RData::ref_rdata(rdata));
					cb->completed(true);
				}
				return xic::ASYNC_ANSWER;
			}
		}
	}

	_memcache->get(cb, key);
	return xic::ASYNC_ANSWER;
}

XIC_METHOD(MCache, getMulti)
{
	xic::VDict args = quest->args();
	std::vector<xstr_t> keys;
	args.wantXstrSeq("keys", keys);

	for (size_t i = 0; i < keys.size(); ++i)
	{
		prepare_key(quest, keys[i]);
	}

	xic::VDict ctx = quest->context();
	int cache = ctx.getInt("CACHE");
	MCallbackPtr cb(new MCacheCallback(MOC_GETMULTI, current.asynchronous(), cache ? _rcache : RCachePtr(), make_string(quest->service())));
	if (cache > 0)
	{
		std::vector<xstr_t> notfoundkeys;
		for (size_t i = 0; i < keys.size(); ++i)
		{
			xstr_t key = keys[i];
			RKey rkey(quest->service(), key);
			RData rdata = _rcache->find(rkey);
			if (rdata && rdata.type() == RD_MCACHE)
			{
				int status = rdata.status();
				// exception answer only cache 1 second.
				uint64_t expire = (status ? 1 : cache) * cpu_frequency();

				if ((rdtsc() - rdata.ctime()) < expire)
				{
					if (status == 0)
					{
						MValue mv;
						mv.key = key;
						mv.value = rdata.xstr();
						mv.revision = 0;
						mv.flags = 0;
						cb->received(&mv, 1, false, RData::unref_rdata, RData::ref_rdata(rdata));
						continue;
					}
				}
			}

			notfoundkeys.push_back(key);
		}

		if (notfoundkeys.empty())
		{
			cb->completed(true);
			return xic::ASYNC_ANSWER;
		}
		else
		{
			notfoundkeys.swap(keys);
		}
	}

	_memcache->getMulti(cb, keys);
	return xic::ASYNC_ANSWER;
}

XIC_METHOD(MCache, whichServer)
{
	xic::VDict args = quest->args();
	xstr_t key = args.wantXstr("key");
	prepare_key(quest, key);

	xic::AnswerWriter aw;
	std::string canonical;
	std::string real = _memcache->whichServer(key, canonical);
	aw.param("real", real);
	aw.param("canonical", canonical);
	return aw;
}

XIC_METHOD(MCache, allServers)
{
	xic::AnswerWriter aw;
	std::vector<std::string> all, bad;
	_memcache->allServers(all, bad);

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

