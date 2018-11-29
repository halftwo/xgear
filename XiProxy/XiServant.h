#ifndef XiServant_h_
#define XiServant_h_

#include "RevServant.h"
#include "BigServant.h"
#include "MyMethodTab.h"
#include "RCache.h"

class XiServant: public RevServant, private XMutex
{
	xic::ProxyPtr _prx;
	BigServantPtr _bigServant;
	RCachePtr _rcache;
	XTimerPtr _timer;
	xatomic_t _call_total;
	xatomic_t _call_underway;
	xatomic_t _rcache_hits;
	time_t _expire_time;
	time_t _last_time;
	int _last_usec;
	std::string _last_method;
	MyMethodTab* _mtab;
public:
	XiServant(const xic::EnginePtr& engine, const std::string& identity, int revision, const xic::ProxyPtr& prx, BigServant* bigServant);
	virtual ~XiServant();

	virtual xic::AnswerPtr process(const xic::QuestPtr& quest, const xic::Current& current);

	virtual void getInfo(xic::VDictWriter& dw);
	void markProxyMethods(xic::AnswerWriter& aw, const xic::QuestPtr& quest);

	void call_end(const xstr_t& method, int usec, bool add);
	const RCachePtr& rcache() const		{ return _rcache; }
	const XTimerPtr& timer() const 		{ return _timer; }
};
typedef XPtr<XiServant> XiServantPtr;


#endif
