#include "BigServant.h"
#include "XiProxy.h"
#include "XiServant.h"
#include "LCache.h"
#include "MCache.h"
#include "Redis.h"
#include "xic/EngineImp.h"
#include "dlog/dlog.h"
#include "xslib/XThread.h"
#include "xslib/hseq.h"
#include "xslib/Enforce.h"
#include <unistd.h>

#define RCACHE_NUM_ITEM		(1024*64)
#define RCACHE_MAX_TIME		(3600*24)


BigServant::BigServant(const xic::EnginePtr& engine, const SettingPtr& setting)
	: _engine(engine), _proxyConfig(setting->wantPathname("XiProxy.ListFile"))
{
	_hint = _map.end();

	_rcache_expire_max = setting->getInt("XiProxy.Cache.ExpireMax");
	if (_rcache_expire_max <= 0)
		_rcache_expire_max = RCACHE_MAX_TIME;
	else if (_rcache_expire_max > 3600*24*365)
		_rcache_expire_max = 3600*24*365;

	int rcache_number_max = setting->getInt("XiProxy.Cache.NumberMax");
	if (rcache_number_max <= 0 || rcache_number_max > INT_MAX)
		rcache_number_max = RCACHE_NUM_ITEM;

	_rcache.reset(new RCache(rcache_number_max));
	_timer = XTimer::create();
	_timer->start();

	xref_inc();
	XThread::create(this, &BigServant::reload_thread);
	XThread::create(this, &BigServant::reap_thread);
	xref_dec_only();
}

BigServant::~BigServant()
{
}

void BigServant::reload_thread()
{
	for (int seconds = 0; true; _engine->sleep(1), ++seconds)
	try 
	{
		if (seconds % 5 == 0 && _proxyConfig.reload())
		{
			Lock sync(*this);
			for (ServantMap::iterator iter = _map.begin(); iter != _map.end(); )
			{
				ProxyDetail pd;
				if (!_proxyConfig.find(iter->second->service(), pd)
					|| iter->second->revision() != pd.revision)
				{
					_map.erase(iter++);
				}
				else
				{
					++iter;
				}
			}
			_hint = _map.end();
		}
	}
	catch (std::exception& ex)
	{
		dlog("ERROR", "%s", ex.what());
	}
}

void BigServant::reap_thread()
{
	while (true)
	try 
	{
		_engine->sleep(60);
		uint64_t before = rdtsc() - (_rcache_expire_max * cpu_frequency());
		size_t n, num = 0;
		const size_t NNN = 10;
		do {
			n = _rcache->reap(NNN, before);
			num += n;
		} while (n >= NNN);

		if (num > 0)
			dlog("RCACHE_REAP", "num=%zd", num);
	}
	catch (std::exception& ex)
	{
		dlog("ERROR", "%s", ex.what());
	}
}

static std::string _reorder_endpoints(const std::string& endpoints, int max)
{
	xstr_t xs = XSTR_CXX(endpoints);
	int num = xstr_count_char(&xs, '@');
	if (num < 2)
		return endpoints;

	hseq_bucket_t *buckets = (hseq_bucket_t *)alloca(num * sizeof(buckets[0]));
	int n = 0;
	xstr_t endpoint;
	while (xstr_delimit_char(&xs, '@', &endpoint))
	{
		xstr_trim(&endpoint);
		if (endpoint.len == 0)
			continue;

		buckets[n].identity = endpoint.data;
		buckets[n].idlen = endpoint.len;
		buckets[n].weight = 0;
		++n;
	}

	if (max < 1)
		max = 1;
	if (max > n)
		max = n;

	int *seqs = (int *)alloca(max * sizeof(int));
	hseq_t *hs = hseq_create(buckets, n);
	ENFORCE(hs);
	hseq_sequence(hs, xp_the_ip, -1, seqs, max);
	std::ostringstream os;
	for (int i = 0; i < max; ++i)
	{
		hseq_bucket_t *b = &buckets[seqs[i]];
		os << '@' << std::string((char *)b->identity, b->idlen);
	}
	hseq_destroy(hs);
	return os.str();
}

RevServantPtr BigServant::_load(const std::string& service)
{
	RevServantPtr srv;
	ProxyDetail pd;
	bool found = _proxyConfig.find(service, pd);
	if (found)
	{
		xstr_t xs = XSTR_CXX(service);
		xstr_t id;
		xstr_delimit_char(&xs, '~', &id);

		if (pd.type == InternalProxy)
		{
			if (xstr_equal_cstr(&id, "MCache"))
			{
				srv.reset(new MCache(_engine, service, pd.revision, pd.value, _rcache));
			}
			else if (xstr_equal_cstr(&id, "Redis"))
			{
				srv.reset(new Redis(_engine, service, pd.revision, pd.value));
			}
		}
		else
		{
			std::string identity = make_string(id);
			std::string endpoints = _reorder_endpoints(pd.value, INT_MAX);
			xic::ProxyPtr prx = _engine->stringToProxy(identity + ' ' + pd.option + endpoints);
			srv.reset(new XiServant(_engine, service, pd.revision, prx, this));
		}

		if (srv)
			_hint = _map.insert(_hint, std::make_pair(service, srv));
	}
	return srv;
}

xic::AnswerPtr BigServant::process(const xic::QuestPtr& quest, const xic::Current& current)
{
	std::string service = make_string(quest->service());
	RevServantPtr srv = find(service, true);
	if (!srv)
		throw XERROR_MSG(xic::ServiceNotFoundException, service);

	return srv->process(quest, current);
}

class Collector: public XRefCount, private XMutex
{
	xic::WaiterPtr _waiter;
	std::vector<xic::AnswerPtr> _answers;
	size_t _left;
public:
	Collector(const xic::WaiterPtr& waiter, size_t num)
		: _waiter(waiter), _answers(num), _left(num)
	{
	}

	void collect(const xic::AnswerPtr& answer, size_t idx)
	{
		Lock lock(*this);
		try {
			if (idx >= _answers.size())
				throw XERROR_MSG(XError, "XiProxy Internal Error");

			_answers[idx] = answer;
			--_left;

			if (_left == 0)
			{
				xic::AnswerWriter aw;
				xic::VListWriter lw = aw.paramVList("answers");
				for (size_t i = 0; i < _answers.size(); ++i)
				{
					xic::AnswerPtr& answer = _answers[i];
					if (!answer)
						throw XERROR_MSG(XError, "XiProxy Internal Error");
					xic::VDictWriter dw = lw.vdict();
					dw.kv("status", answer->status());
					dw.kv("a", answer->args());
				}

				_waiter->response(aw);
			}
		}
		catch (std::exception& ex)
		{
			_waiter->response(ex);
		}
	}
};
typedef XPtr<Collector> CollectorPtr;

class SalvoFakeWaiter: public xic::WaiterI
{
	CollectorPtr _collector;
	size_t _idx;
public:
	SalvoFakeWaiter(const xic::CurrentI& r, const CollectorPtr& col, size_t idx)
		: xic::WaiterI(r), _collector(col), _idx(idx)
	{
	}

	virtual bool responded() const 		{ return false; }

        virtual void response(const xic::AnswerPtr& answer, bool trace)
	{
		_collector->collect((answer->status() && trace) ? this->trace(answer) : answer, _idx);
	}
};

class SalvoFakeCurrent: public xic::CurrentI
{
	CollectorPtr _collector;
	size_t _idx;
public:
	SalvoFakeCurrent(const xic::Current& c, const xic::QuestPtr& q, const CollectorPtr& col, size_t idx)
		: CurrentI(c.con.get(), q.get()), _collector(col), _idx(idx)
	{
	}

	virtual xic::WaiterPtr asynchronous() const
	{
		if (!_waiter)
		{
			_waiter.reset(new SalvoFakeWaiter(*this, _collector, _idx)); 
		}
		return _waiter;
	}
};

xic::AnswerPtr BigServant::salvo(const xic::QuestPtr& quest, const xic::Current& current)
{
	xic::VDict args = quest->args();
	xic::VList qs = args.wantVList("quests");
	CollectorPtr collector(new Collector(current.asynchronous(), qs.size()));
	xic::ContextBuilder ctxBuilder(quest->context());
	ctxBuilder.set("SALVO", true);
	xic::ContextPtr ctx = ctxBuilder.build();
	size_t idx = 0;
	for (xic::VList::Node node = qs.first(); node; ++node, ++idx)
	{
		xic::VDict qdict = node.vdictValue();
		xstr_t s = qdict.wantXstr("s");
		xstr_t m = qdict.wantXstr("m");
		const vbs_dict_t *p = qdict.want_dict("a");

		xic::AnswerPtr answer;
		try
		{
			std::string sxx = make_string(s);
			xic::ServantPtr srv = find(sxx, true);

			if (!srv)
			{
				srv = current.con->getAdapter()->findServant(sxx);
				if (!srv)
					throw XERROR_MSG(xic::ServiceNotFoundException, sxx);
			}

			xic::QuestWriter qw(m);
			xstr_t raw = xstr_slice(&p->_raw, 1, -1);
			qw.raw(raw.data, raw.len);
			xic::QuestPtr q = qw.take();
			q->setService(s);
			q->setContext(ctx);

			SalvoFakeCurrent fake_current(current, q, collector, idx);
			answer = srv->process(q, fake_current);
		}
		catch (std::exception& ex)
		{
			answer = xic::except2answer(ex, s, m, current.con->endpoint());
		}

		if (answer != xic::ASYNC_ANSWER)
		{
			collector->collect(answer, idx);
		}
	}
	return xic::ASYNC_ANSWER;
}

RevServantPtr BigServant::find(const std::string& service, bool load)
{
	RevServantPtr srv;

	Lock lock(*this);
	ServantMap::iterator iter = _hint;
	if (iter != _map.end() && iter->first == service)
	{
		srv = iter->second;
	}
	else
	{
		iter = _map.find(service);
		if (iter != _map.end())
		{
			_hint = iter;
			srv = iter->second;
		}
		else if (load)
		{
			srv = _load(service);
		}
	}
	return srv;
}

void BigServant::remove(const std::string& service)
{
	Lock lock(*this);
	_map.erase(service);
	_hint = _map.end();
}

xic::AnswerPtr BigServant::stats(const xic::QuestPtr& quest, const xic::Current& current)
{
	xic::AnswerWriter aw;
	xic::VListWriter lw = aw.paramVList("services");
	{
		Lock lock(*this);
		for (ServantMap::iterator iter = _map.begin(); iter != _map.end(); ++iter)
		{
			lw.v(iter->first);
		}
	}
	return aw;
}

xic::AnswerPtr BigServant::getProxyInfo(const xic::QuestPtr& quest, const xic::Current& current)
{
	xic::QuestReader qr(quest);
	std::string service = make_string(qr.wantXstr("service"));
	RevServantPtr srv = find(service, false);

	xic::AnswerWriter aw;
	if (srv)
	{
		char time_buf[32];
		aw.param("now", xp_get_time_str(_engine->time(), time_buf));
		aw.param("service", service);
		xic::VDictWriter dw = aw.paramVDict("info");
		srv->getInfo(dw);
	}
	return aw;
}

xic::AnswerPtr BigServant::markProxyMethods(const xic::QuestPtr& quest, const xic::Current& current)
{
	xic::QuestReader qr(quest);
	std::string service = make_string(qr.wantXstr("service"));
	RevServantPtr srv = find(service, true);

	xic::AnswerWriter aw;
	if (srv)
	{
		XiServantPtr x = XiServantPtr::cast(srv);
		if (x)
			x->markProxyMethods(aw, quest);
	}
	return aw;
}

void BigServant::shutdown()
{
	_engine->shutdown();
}


