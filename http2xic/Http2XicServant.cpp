#include "Http2XicServant.h"
#include "xslib/xlog.h"
#include "xslib/ScopeGuard.h"
#include "dlog/dlog.h"
#include <stdio.h>

Http2XicServant::MethodTab::PairType Http2XicServant::_methodpairs[] = {
#define CMD(X)  { #X, XIC_METHOD_CAST(Http2XicServant, X) },
	HTTP2XIC_CMDS
#undef CMD
};

Http2XicServant::MethodTab Http2XicServant::_methodtab(_methodpairs, XS_ARRCOUNT(_methodpairs));


Http2XicServant::Http2XicServant(const xic::AdapterPtr& adapter, const SettingPtr& setting)
	: ServantI(&_methodtab)
{
	_engine = adapter->getEngine();
	_start_time = time(NULL);
}

Http2XicServant::~Http2XicServant()
{
}

XIC_METHOD(Http2XicServant, status)
{
	xic::AnswerWriter aw;
	aw.param("start", _start_time);
#ifdef __BUILD_TIME
	aw.param("build", __BUILD_TIME);
#endif
	return aw.take();
}

