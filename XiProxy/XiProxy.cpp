#include "HttpHandler.h"
#include "RevServant.h"
#include "BigServant.h"
#include "XiServant.h"
#include "RCache.h"
#include "LCache.h"
#include "MCache.h"
#include "Redis.h"
#include "Dlog.h"
#include "Quickie.h"
#include "xic/ServantI.h"
#include "dlog/dlog.h"
#include "xslib/xlog.h"
#include "xslib/rdtsc.h"
#include "xslib/xnet.h"
#include "xslib/XRefCount.h"
#include "xslib/XLock.h"
#include "xslib/XError.h"
#include "xslib/XTimer.h"
#include "xslib/Enforce.h"
#include "xslib/Setting.h"
#include <unistd.h>
#include <map>
#include <string>

#define XIPROXY_VERSION		"22102820"

static char build_info[] = "$build: XiProxy-" XIPROXY_VERSION " " __DATE__ " " __TIME__ " $";

#define LOG_LEVEL_DEFAULT	1
#define ULTRA_SLOW_MSEC_DEFAULT	66666
#define SLOW_MSEC_DEFAULT	1000
#define REFRESH_TIME_DEFAULT	(3600*1)
#define REFRESH_TIME_MIN	60

char xp_the_ip[64];
int xp_log_level = LOG_LEVEL_DEFAULT;
int64_t xp_ultra_slow_msec = ULTRA_SLOW_MSEC_DEFAULT;
int64_t xp_slow_warning_msec = SLOW_MSEC_DEFAULT;
unsigned int xp_refresh_time = REFRESH_TIME_DEFAULT;
int xp_delay_msec = 0;


char *xp_get_time_str(time_t t, char *buf)
{
	return dlog_local_time_str(buf, t, true);
}


class XiProxyCtrl: public xic::Servant, private XMutex
{
	BigServantPtr _bigsrv;
public:
	XiProxyCtrl(const BigServantPtr& bigsrv)
		: _bigsrv(bigsrv)
	{
	}

	virtual xic::AnswerPtr process(const xic::QuestPtr& quest, const xic::Current& current);
};

xic::AnswerPtr XiProxyCtrl::process(const xic::QuestPtr& quest, const xic::Current& current)
{
	const xstr_t& method = quest->method();

	if (xstr_equal_cstr(&method, "stats"))
	{
		return _bigsrv->stats(quest, current);
	}
	else if (xstr_equal_cstr(&method, "getProxyInfo"))
	{
		return _bigsrv->getProxyInfo(quest, current);
	}
	else if (xstr_equal_cstr(&method, "markProxyMethods"))
	{
		return _bigsrv->markProxyMethods(quest, current);
	}
	else if (xstr_equal_cstr(&method, "clearCache"))
	{
		_bigsrv->clearCache();
		return xic::AnswerWriter();
	}

	throw XERROR_MSG(xic::MethodNotFoundException, make_string(method));
	return xic::ONEWAY_ANSWER;
}

static int run(int argc, char **argv, const xic::EnginePtr& engine)
{
	srandom(time(NULL) ^ getpid());
	engine->throb(build_info);
	SettingPtr setting = engine->setting();

	uint32_t ip;
	if (xnet_ipv4_get_all(&ip, 1) >= 1)
		xnet_ipv4_ntoa(ip, xp_the_ip);
	else
		sprintf(xp_the_ip, ":%u", (unsigned int)getpid());

	xp_log_level = setting->getInt("XiProxy.Service.LogLevel", LOG_LEVEL_DEFAULT);

	xp_slow_warning_msec = setting->getInt("XiProxy.Service.Slow", SLOW_MSEC_DEFAULT);
	if (xp_slow_warning_msec < 0)
		xp_slow_warning_msec = INT64_MAX;

	xp_ultra_slow_msec = setting->getInt("XiProxy.Service.UltraSlow", ULTRA_SLOW_MSEC_DEFAULT);
	if (xp_ultra_slow_msec < 0)
		xp_ultra_slow_msec = INT64_MAX;
	else if (xp_ultra_slow_msec < xp_slow_warning_msec)
		xp_ultra_slow_msec = xp_slow_warning_msec;

	xp_refresh_time = setting->getInt("XiProxy.Service.RefreshTime", REFRESH_TIME_DEFAULT);
	if (xp_refresh_time < REFRESH_TIME_MIN)
		xp_refresh_time = REFRESH_TIME_MIN;

	// NB: This will cause each call of XiProxy delay the specified milliseconds.
	// Do NOT set this value above 0 in production environment.
	xp_delay_msec = setting->getInt("XiProxy.Service.Delay", 0);

	xic::AdapterPtr adapter = engine->createAdapter();
	if (setting->getString("XiProxy.ListFile").empty())
		throw XERROR_MSG(XError, "XiProxy.ListFile is required to be set in configuration");

	BigServantPtr bigsrv(new BigServant(engine, setting));
	XPtr<Dlog> dlogger(new Dlog());
	adapter->addServant("Dlog", dlogger);
	adapter->addServant("LCache", new LCache(bigsrv->rcache()));
	adapter->addServant("Quickie", new Quickie(bigsrv));
	adapter->addServant("XiProxyCtrl", new XiProxyCtrl(bigsrv));
	adapter->setDefaultServant(bigsrv);

	HttpHandlerPtr httpHandler;
	if (setting->getInt("XiProxy.Http.Port") > 0)
		httpHandler = new HttpHandler(engine, adapter);

	adapter->activate();
	if (httpHandler)
		httpHandler->start();

	engine->waitForShutdown();

	if (httpHandler)
		httpHandler->stop();
	return 0;
}

int main(int argc, char **argv)
{
	SettingPtr setting = newSetting();
	setting->insert("xic.rlimit.nofile", "8192");
	return xic::start_xic_pt(run, argc, argv, setting);
}

