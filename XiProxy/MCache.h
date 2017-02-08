#ifndef MCache_h_
#define MCache_h_

#include "RCache.h"
#include "Memcache.h"
#include "RevServant.h"
#include "xic/ServantI.h"


#define MCACHE_CMDS		\
	CMD(set)		\
	CMD(replace)		\
	CMD(add)		\
	CMD(append)		\
	CMD(prepend)		\
	CMD(cas)		\
	CMD(get)		\
	CMD(getMulti)		\
	CMD(delete)		\
	CMD(increment)		\
	CMD(decrement)		\
	CMD(whichServer)	\
	CMD(allServers)		\
	/* END OF CMDS */

class MCache: public RevServant, private XMutex
{
	static xic::MethodTab::PairType _funpairs[];
	static xic::MethodTab _funtab;

	std::string _servers;
	RCachePtr _rcache;
	MemcachePtr _memcache;
public:
	MCache(const xic::EnginePtr& engine, const std::string& service, int revision, const std::string& servers, const RCachePtr& rcache);
	virtual ~MCache();

	virtual xic::AnswerPtr process(const xic::QuestPtr& quest, const xic::Current& current);
	virtual void getInfo(xic::VDictWriter& dw);

private:
#define CMD(X) XIC_METHOD_DECLARE(X);
	MCACHE_CMDS
#undef CMD
};


#endif
