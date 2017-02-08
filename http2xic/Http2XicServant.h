#ifndef Http2XicServant_h_
#define Http2XicServant_h_

#include "xic/ServantI.h"

class Http2XicServant;
typedef XPtr<Http2XicServant> Http2XicServantPtr;


#define HTTP2XIC_CMDS		\
	CMD(status)		\
	/* END */


class Http2XicServant: public xic::ServantI
{
	static MethodTab::PairType _methodpairs[];
	static MethodTab _methodtab;

	xic::EnginePtr _engine;
	time_t _start_time;
public:
	Http2XicServant(const xic::AdapterPtr& adapter, const SettingPtr& setting);
	virtual ~Http2XicServant();

#define CMD(X)	XIC_METHOD_DECLARE(X);
	HTTP2XIC_CMDS
#undef CMD
};

#endif

