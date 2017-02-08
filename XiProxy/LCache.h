#ifndef LCache_h_
#define LCache_h_

#include "xic/ServantI.h"
#include "RCache.h"

#define LCACHE_CMDS		\
	CMD(set)		\
	CMD(get)		\
	CMD(getAll)		\
	CMD(plus)		\
	CMD(remove)		\
	CMD(remove_answer)	\
	CMD(get_answer)		\
	CMD(remove_mcache)	\
	CMD(get_mcache)		\
	/* END OF CMDS */

class LCache: public xic::ServantI
{
	static xic::MethodTab::PairType _funpairs[];
	static xic::MethodTab _funtab;

	RCachePtr _rcache;
public:
	LCache(const RCachePtr& rcache);
	virtual ~LCache();

private:
#define CMD(X) XIC_METHOD_DECLARE(X);
	LCACHE_CMDS
#undef CMD
};

#endif
