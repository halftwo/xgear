#include "XiServant.h"
#include "XiProxy.h"
#include "dlog/dlog.h"
#include "xslib/xlog.h"
#include "xslib/rdtsc.h"
#include <string.h>

XiServant::XiServant(const xic::EnginePtr& engine, const std::string& identity, int revision,
	const xic::ProxyPtr& prx, BigServant* bigServant)
	: RevServant(engine, identity, revision), _prx(prx), _bigServant(bigServant),
		_rcache(bigServant->rcache()), _timer(bigServant->timer())
{
	xatomic_set(&_call_total, 0);
	xatomic_set(&_call_underway, 0);
	xatomic_set(&_rcache_hits, 0);
	_expire_time = _start_time + (time_t)(xp_refresh_time * (1.0 + 0.1 * random() / RAND_MAX));
	_last_time = 0;
	_last_usec = 0;
}

XiServant::~XiServant()
{
}

class XiServantCompletion: public xic::Completion
{
	XiServantPtr _ksrv;
	xic::WaiterPtr _waiter;
	uint64_t _start_tsc;
	RKey _rkey;
	int _cache;
public:
	XiServantCompletion(XiServant *ksrv, const xic::WaiterPtr& waiter, int cache, const RKey& rkey)
		: _ksrv(ksrv), _waiter(waiter), _rkey(rkey), _cache(cache)
	{
		_start_tsc = rdtsc();
	}

	virtual void completed(const xic::ResultPtr& result);
};

class DelayedResponse: public XTimerTask
{
	xic::WaiterPtr _waiter;
	xic::AnswerPtr _answer;
public:
	DelayedResponse(const xic::WaiterPtr& waiter, const xic::AnswerPtr& answer)
		: _waiter(waiter), _answer(answer)
	{
	}

	virtual void runTimerTask(const XTimerPtr& timer)
	{
		_waiter->response(_answer);
	}
};

void XiServantCompletion::completed(const xic::ResultPtr& result)
{
	xic::Quest* q = result->quest().get();
	xic::AnswerPtr answer = result->takeAnswer(false);
	uint64_t current_tsc = rdtsc();
	int64_t used_usec = (current_tsc - _start_tsc) * 1000000 / cpu_frequency();

	if (xp_delay_msec <= 0)
	{
		_waiter->response(answer);
	}
	else
	{
		_ksrv->timer()->addTask(new DelayedResponse(_waiter, answer), xp_delay_msec);
	}

	_ksrv->call_end(q->method(), used_usec);

	if (_cache)
	{
		const RCachePtr& rcache = _ksrv->rcache();
		RData rdata(current_tsc, RD_ANSWER, answer->args_xstr());
		if (rdata)
		{
			rdata.setStatus(answer->status());
			rcache->replace(_rkey, rdata);
		}
		else
		{
			rcache->remove(_rkey);
		}
	}

	int status = answer->status();
	int64_t used_ms = used_usec / 1000;
	if ((status && xp_log_level > 0) || used_ms >= xp_slow_warning_msec)
	{
		intmax_t txid = q->txid();
		const xstr_t& service = q->service();
		const xstr_t& method = q->method();
		char caution_locus[128], *lp = caution_locus;

		xstr_t c0 = xstr_null;
		xic::ConnectionPtr con0 = _waiter->getConnection();
		if (con0)
			xstr_cxx(&c0, con0->info());

		xstr_t c1 = xstr_null;
		xic::ConnectionPtr con1 = result->getConnection();
		if (con1)
			xstr_cxx(&c1, con1->info());

		xstr_t client, me0, me1, server;
		xstr_delimit_char(&c0, '/', NULL);
		xstr_delimit_char(&c0, '/', &me0);
		xstr_delimit_char(&c0, '/', &client);

		xstr_delimit_char(&c1, '/', NULL);
		xstr_delimit_char(&c1, '/', &me1);
		xstr_delimit_char(&c1, '/', &server);

		me0 = xstr_substr(&me0, xstr_rfind_char(&me0, -1, '+') + 1, XSTR_MAXLEN);
		me1 = xstr_substr(&me1, xstr_rfind_char(&me1, -1, '+') + 1, XSTR_MAXLEN);

		caution_locus[0] = 0;
		if (used_ms >= xp_slow_warning_msec)
		{
			lp = stpcpy(lp, "/XP_SLOW");

			if (used_ms >= xp_ultra_slow_msec)
			{
				lp = stpcpy(lp, "/XP_ULTRA_SLOW");
				// Close the connection. Let the xic engine make another connnection
				// (using another endpoint).
				xic::ConnectionPtr con = result->getConnection();
				if (con)
					con->close(false);
			}
		}

		if (status && xp_log_level > 0)
		{
			if (xp_log_level > 1)
			{
				lp = stpcpy(lp, "/XP_ERR");
			}

			xdlog(vbs_xfmt, NULL, "XP_EXCEPT", NULL,
				"T=%d.%03d con=%.*s/%.*s,%.*s/%.*s %jd Q=%.*s::%.*s C%p{>VBS_RAW<} A=%d %p{>VBS_RAW<}",
				(int)(used_ms/1000), (int)(used_ms%1000),
				XSTR_P(&client), XSTR_P(&me0), XSTR_P(&me1), XSTR_P(&server),
				txid, XSTR_P(&service), XSTR_P(&method),
				&q->context_xstr(),
				status, &answer->args_xstr());
		}

		if (lp > caution_locus)
		{
			*lp++ = '/';
			*lp = 0;
			xdlog(vbs_xfmt, NULL, "XP_CAUTION", caution_locus,
				"T=%d.%03d con=%.*s/%.*s,%.*s/%.*s %jd Q=%.*s::%.*s C%p{>VBS_RAW<} %p{>VBS_RAW<} A=%d %p{>VBS_RAW<}",
				(int)(used_ms/1000), (int)(used_ms%1000),
				XSTR_P(&client), XSTR_P(&me0), XSTR_P(&me1), XSTR_P(&server),
				txid, XSTR_P(&service), XSTR_P(&method),
				&q->context_xstr(), &q->args_xstr(),
				status, &answer->args_xstr());
		}
	}
}

static void free_rope_rdata(void *cookie, void *buf)
{
	RData::unref_rdata(cookie);
}

xic::AnswerPtr XiServant::process(const xic::QuestPtr& quest, const xic::Current& current)
{
	xic::CompletionPtr cb;

	xatomic_inc(&_call_total);
	if (quest->txid())
	{
		xic::VDict ctx = quest->context();
		int cache = ctx.getInt("CACHE");
		RKey rkey;
		if (!cache)
			goto no_cache;

		rkey.set(quest->service(), quest->method(), quest->args_xstr());
		if (cache > 0)
		{
			RData rdata = _rcache->find(rkey);
			if (rdata.type() == RD_ANSWER)
			{
				int status = rdata.status();
				// exception answer only cache 1 second.
				uint64_t expire = (status ? 1 : cache) * cpu_frequency();
				xstr_t xs = rdata.xstr();
				if ((rdtsc() - rdata.ctime()) < expire && 
					xs.len >= 2 && xs.data[xs.len-1] == VBS_TAIL)
				{
					xatomic_inc(&_rcache_hits);
					xic::AnswerPtr answer = xic::Answer::create();
					answer->setStatus(status);
					rope_t *rope = answer->args_rope();
					void *cookie = RData::ref_rdata(rdata);
					if (rope_append_external(rope, xs.data, xs.len, free_rope_rdata, cookie))
					{
						return answer;
					}
					else
					{
						RData::unref_rdata(cookie);
						dlog("FATAL", "rope_append_external() failed");
					}
				}
			}
		}
	
	no_cache:
		xatomic_inc(&_call_underway);
		cb.reset(new XiServantCompletion(this, current.asynchronous(), cache, rkey));
	}

	if (_serviceChanged)
		quest->setService(_origin);

	time_t now = _engine->time();
	if (_prx->loadBalance() == xic::Proxy::LB_NORMAL && now > _expire_time)
	{
		Lock lock(*this);
		if (now > _expire_time)
		{
			_expire_time = now + (time_t)(xp_refresh_time * (1.0 + 0.1 * random() / RAND_MAX));
			_prx->resetConnection();
		}
	}

	_prx->emitQuest(quest, cb);
	return xic::ASYNC_ANSWER;
}

void XiServant::call_end(const xstr_t& method, int usec)
{
	xatomic_dec(&_call_underway);
	{
		Lock lock(*this);
		_last_method = make_string(method);
		_last_time = _engine->time();
		_last_usec = usec;
	}
}

void XiServant::getInfo(xic::VDictWriter& dw)
{
	char buf[256];

	RevServant::getInfo(dw);

	dw.kv("type", "external");
	dw.kv("proxy", _prx->str());
	dw.kv("age", _engine->time() - _start_time);
	dw.kv("expire_time", xp_get_time_str(_expire_time, buf));
	xic::ConnectionPtr con = _prx->getConnection();
	if (con)
	{
		snprintf(buf, sizeof(buf), "%s/%d", con->info().c_str(), con->state());
	}
	else
	{
		buf[0] = 0;
	}
	dw.kv("connection", buf);
	dw.kv("num_rcache_hit", xatomic_get(&_rcache_hits));
	dw.kv("num_call_total", xatomic_get(&_call_total));
	dw.kv("num_call_underway", xatomic_get(&_call_underway));

	std::string last_method;
	time_t last_time;
	int last_usec;
	{
		Lock lock(*this);
		last_method = _last_method;
		last_time = _last_time;
		last_usec = _last_usec;
	}

	dw.kv("last_call_method", last_method);
	dw.kv("last_call_time", last_time ? xp_get_time_str(last_time, buf) : "");
	dw.kv("last_call_usec", last_usec);
}

