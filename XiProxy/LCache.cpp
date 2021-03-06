#include "LCache.h"
#include "xslib/rdtsc.h"

xic::MethodTab::PairType LCache::_funpairs[] = {
#define CMD(X)	{ #X, XIC_METHOD_CAST(LCache, X) },
	LCACHE_CMDS
#undef CMD
};

xic::MethodTab LCache::_funtab(_funpairs, XS_ARRCOUNT(_funpairs));


LCache::LCache(const RCachePtr& rcache)
	: ServantI(&_funtab), _rcache(rcache)
{
}

LCache::~LCache()
{
}

XIC_METHOD(LCache, get)
{
	xic::QuestReader qr(quest);
	const xstr_t& key = qr.wantXstr("key");
	long maxage = qr.getInt("maxage");

	RKey rkey(key);
	RData d = _rcache->use(rkey);
	long age = d ? (rdtsc() - d.ctime()) / cpu_frequency() : LONG_MAX;

	xic::AnswerWriter aw;
	if (d && (!maxage || age < maxage) && d.type() == RD_LCACHE)
	{
		aw.paramStanza("value", d.data(), d.length());
		aw.param("age", age);
	}
	else
		aw.paramNull("value");
	return aw;
}

XIC_METHOD(LCache, set)
{
	xic::QuestReader qr(quest);
	const xstr_t& key = qr.wantXstr("key");
	const vbs_data_t* value = qr.want_data("value");

	RKey rkey(key);
	if (value->kind != VBS_NULL)
	{
		RData d(rdtsc(), RD_LCACHE, *value);
		_rcache->replace(rkey, d);
	}
	else
	{
		_rcache->remove(rkey);
	}
	return xic::AnswerWriter();
}

XIC_METHOD(LCache, get_or_set)
{
	xic::QuestReader qr(quest);
	const xstr_t& key = qr.wantXstr("key");
	const vbs_data_t* value = qr.want_data("value");
	long maxage = qr.getInt("maxage");

	RKey rkey(key);
	RData d = _rcache->use(rkey);
	long age = d ? (rdtsc() - d.ctime()) / cpu_frequency() : LONG_MAX;

	xic::AnswerWriter aw;
	if (d && (!maxage || age < maxage) && d.type() == RD_LCACHE)
	{
		aw.paramStanza("value", d.data(), d.length());
		aw.param("age", age);
	}
	else
	{
		aw.paramNull("value");
		if (value->kind != VBS_NULL)
		{
			RData newdata(rdtsc(), RD_LCACHE, *value);
			_rcache->replace(rkey, newdata);
		}
		else
		{
			_rcache->remove(rkey);
		}
	}
	return aw;
}

XIC_METHOD(LCache, get_and_set)
{
	xic::QuestReader qr(quest);
	const xstr_t& key = qr.wantXstr("key");
	const vbs_data_t* value = qr.want_data("value");
	long maxage = qr.getInt("maxage");

	RKey rkey(key);
	RData d = _rcache->use(rkey);
	long age = d ? (rdtsc() - d.ctime()) / cpu_frequency() : LONG_MAX;

	// get
	xic::AnswerWriter aw;
	if (d && (!maxage || age < maxage) && d.type() == RD_LCACHE)
	{
		aw.paramStanza("value", d.data(), d.length());
		aw.param("age", age);
	}
	else
	{
		aw.paramNull("value");
	}

	// set
	if (value->kind != VBS_NULL)
	{
		RData newdata(rdtsc(), RD_LCACHE, *value);
		_rcache->replace(rkey, newdata);
	}
	else
	{
		_rcache->remove(rkey);
	}
	return aw;
}

XIC_METHOD(LCache, remove)
{
	xic::QuestReader qr(quest);
	const xstr_t& key = qr.wantXstr("key");
	RKey rkey(key);
	bool ok = _rcache->remove(rkey);
	return xic::AnswerWriter()("ok", ok);
}

XIC_METHOD(LCache, getAll)
{
	xic::QuestReader qr(quest);
	std::vector<xstr_t> keys;
	qr.wantXstrSeq("keys", keys);
	long maxage = qr.getInt("maxage");

	uint64_t after = 0;
	if (maxage > 0)
	{
		uint64_t now = rdtsc();
		after = now - maxage * cpu_frequency();
		if (after > now)
			after = 0;
	}

	xic::AnswerWriter aw;
	xic::VDictWriter dw = aw.paramVDict("items");
	size_t size = keys.size();
	for (size_t i = 0; i < size; ++i)
	{
		const xstr_t& key = keys[i];
		RKey rkey(key);
		RData d = _rcache->use(rkey);
		if (d && d.ctime() > after && d.type() == RD_LCACHE)
		{
			dw.kvstanza(key, d.data(), d.length());
		}
	}
	return aw;
}

XIC_METHOD(LCache, plus)
{
	xic::QuestReader qr(quest);
	const xstr_t& key = qr.wantXstr("key");
	intmax_t value = qr.wantInt("value");
	long maxage = qr.getInt("maxage");

	uint64_t now = rdtsc();
	uint64_t after = 0;
	if (maxage > 0)
	{
		after = now - maxage * cpu_frequency();
		if (after > now)
			after = 0;
	}

	RKey rkey(key);
	value = _rcache->plus(rkey, value, now, after);
	return xic::AnswerWriter()("value", value);
}

XIC_METHOD(LCache, remove_answer)
{
	xic::QuestReader qr(quest);
	const xstr_t& s = qr.wantXstr("s");
	const xstr_t& m = qr.wantXstr("m");
	const vbs_dict_t *p = qr.want_dict("a");
	RKey rkey(s, m, p->_raw);
	bool ok = _rcache->remove(rkey);
	return xic::AnswerWriter()("ok", ok);
}

XIC_METHOD(LCache, get_answer)
{
	xic::QuestReader qr(quest);
	const xstr_t& s = qr.wantXstr("s");
	const xstr_t& m = qr.wantXstr("m");
	const vbs_dict_t *p = qr.want_dict("a");
	RKey rkey(s, m, p->_raw);
	RData d = _rcache->find(rkey);
	long age = d ? (rdtsc() - d.ctime()) / cpu_frequency() : LONG_MAX;

	xic::AnswerWriter aw;
	if (d && d.type() == RD_ANSWER)
	{
		aw.paramStanza("value", d.data(), d.length());
		aw.param("age", age);
	}
	else
		aw.paramNull("value");
	return aw;
}

XIC_METHOD(LCache, remove_mcache)
{
	xic::QuestReader qr(quest);
	const xstr_t& s = qr.wantXstr("s");
	const xstr_t& k = qr.wantXstr("k");
	RKey rkey(s, k);
	bool ok = _rcache->remove(rkey);
	return xic::AnswerWriter()("ok", ok);
}

XIC_METHOD(LCache, get_mcache)
{
	xic::QuestReader qr(quest);
	const xstr_t& s = qr.wantXstr("s");
	const xstr_t& k = qr.wantXstr("k");
	RKey rkey(s, k);
	RData d = _rcache->find(rkey);
	long age = d ? (rdtsc() - d.ctime()) / cpu_frequency() : LONG_MAX;

	xic::AnswerWriter aw;
	if (d && d.type() == RD_MCACHE)
	{
		aw.paramBlob("value", d.data(), d.length());
		aw.param("age", age);
	}
	return aw;
}

