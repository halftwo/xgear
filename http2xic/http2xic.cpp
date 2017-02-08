#include "HttpHandler.h"
#include "Http2XicServant.h"
#include "xic/Engine.h"

#define HTTP2XIC_VERSION	"20161021.20161021.2150"

static char build_info[] = "$build: http2xic-"HTTP2XIC_VERSION" "__DATE__" "__TIME__" $";

static int run(int argc, char **argv, const xic::EnginePtr& engine)
{
	xic::AdapterPtr adapter = engine->createAdapter();
	new Http2XicServant(adapter, engine->setting());
	HttpHandlerPtr handler(new HttpHandler(engine));
	adapter->activate();
	handler->start();
	engine->throb(build_info);
	engine->waitForShutdown();
	handler->stop();
	return 0;
}

int main(int argc, char **argv)
{
	SettingPtr setting = newSetting();
	setting->insert("xic.rlimit.nofile", "8192");
	return xic::start_xic_pt(run, argc, argv, setting);
}

