#ifndef Quickie_h_
#define Quickie_h_

#include "xic/ServantI.h"
#include "BigServant.h"

#define QUICKIE_CMDS	\
	CMD(time)	\
	CMD(sink)	\
	CMD(echo)	\
	CMD(hseq)	\
	CMD(salvo)	\
	/* END OF CMDS */

class Quickie: public xic::ServantI
{
	static xic::MethodTab::PairType _funpairs[];
	static xic::MethodTab _funtab;
	
	BigServantPtr _bigsrv;
public:
	Quickie(const BigServantPtr& bigsrv);
	virtual ~Quickie();

#define CMD(X) XIC_METHOD_DECLARE(X);
	QUICKIE_CMDS
#undef CMD
};

#endif

