#ifndef BigServant_h_
#define BigServant_h_

#include "RevServant.h"
#include "ProxyConfig.h"
#include "RCache.h"
#include "xic/ServantI.h"
#include "xslib/XTimer.h"

class BigServant: public xic::Servant, private XMutex
{
	typedef std::map<std::string, RevServantPtr> ServantMap;
	xic::EnginePtr _engine;
	ServantMap _map;
	ServantMap::iterator _hint;
	ProxyConfig _proxyConfig;
	RCachePtr _rcache;
	XTimerPtr _timer;
	int _rcache_expire_max;
public:
	BigServant(const xic::EnginePtr& engine, const SettingPtr& setting);
	virtual ~BigServant();

	virtual xic::AnswerPtr process(const xic::QuestPtr& quest, const xic::Current& current);
	virtual xic::AnswerPtr salvo(const xic::QuestPtr& quest, const xic::Current& current);

	RevServantPtr find(const std::string& service, bool load);
	void remove(const std::string& service);

	RCachePtr rcache() const 	{ return _rcache; }
	XTimerPtr timer() const 	{ return _timer; }

	xic::AnswerPtr stats(const xic::QuestPtr& quest, const xic::Current& current);
	xic::AnswerPtr getProxyInfo(const xic::QuestPtr& quest, const xic::Current& current);
	xic::AnswerPtr markProxyMethods(const xic::QuestPtr& quest, const xic::Current& current);
	void clearCache()		{ _rcache->clear(); }
	void shutdown();

private:
	RevServantPtr _load(const std::string& service);
	void reload_thread();
	void reap_thread();
};
typedef XPtr<BigServant> BigServantPtr;

#endif
