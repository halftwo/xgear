#ifndef Redis_h_
#define Redis_h_

#include "RedisGroup.h"
#include "RevServant.h"
#include "xic/ServantI.h"


#define REDIS_CMDS		\
	CMD(_1CALL)		\
	CMD(_NCALL)		\
	CMD(_TCALL)		\
	CMD(set)		\
	CMD(delete)		\
	CMD(increment)		\
	CMD(decrement)		\
	CMD(get)		\
	CMD(getMulti)		\
	CMD(whichServer)	\
	CMD(allServers)		\
	/* END OF CMDS */

class Redis: public RevServant, private XMutex
{
	static xic::MethodTab::PairType _funpairs[];
	static xic::MethodTab _funtab;

	std::string _servers;
	RedisGroupPtr _redisgroup;
public:
	Redis(const xic::EnginePtr& engine, const std::string& service, int revision, const std::string& servers);
	virtual ~Redis();

	virtual xic::AnswerPtr process(const xic::QuestPtr& quest, const xic::Current& current);
	virtual void getInfo(xic::VDictWriter& dw);

private:
#define CMD(X) XIC_METHOD_DECLARE(X);
	REDIS_CMDS
#undef CMD
};


#endif
