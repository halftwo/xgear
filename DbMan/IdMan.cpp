#include "IdManServant.h"
#include "dlog/dlog.h"
#include "xslib/xlog.h"
#include "xslib/ScopeGuard.h"

#define IDMAN_VERSION	"20161021.20170215.2200"

static char build_info[] = "$build: IdMan-"IDMAN_VERSION" "__DATE__" "__TIME__" $";


static bool check_singleton(const xic::EnginePtr& engine, const IdManServantPtr& idman)
{
	std::string db_idman = idman->get_db_idman();
	if (!db_idman.empty())
	{
		std::string prefix = db_idman.substr(0, 5);
		if (strcasecmp(prefix.c_str(), "IdMan") != 0)
			return false;

		try {
			xic::ProxyPtr prx = engine->stringToProxy(db_idman);
			prx = prx->timedProxy(60*1000);
			xic::QuestWriter qw("ctrl");
			prx->request(qw.take());
			return false;
		}
		catch (std::exception& ex)
		{
			dlog("TRACE", "IdMan=%s exception=%s", db_idman.c_str(), ex.what());
		}
	}

	dlog("TRACE", "It seems that I am the only IdMan!");
	return true;
}

static int run(int argc, char **argv, const xic::EnginePtr& engine)
{
	engine->throb(build_info);

	IdManServantPtr idman(new IdManServant(engine->setting()));
	if (!check_singleton(engine, idman))
		throw XERROR_MSG(XError, "Another IdMan is running!");

	xic::AdapterPtr adapter = engine->createAdapter();
	xic::ProxyPtr prx = adapter->addServant("IdMan", idman);
	idman->setSelfProxy(prx);

	adapter->activate();
	engine->waitForShutdown();
	dlog("TRACE", "IdMan stop and sync to db");
	idman->stop();
	idman->sync_all_to_db();
	return 0;
}

int main(int argc, char **argv)
{
	SettingPtr setting = newSetting();
	setting->insert("xic.rlimit.nofile", "8192");
	setting->insert("xic.sample.server", "1");
	return xic::start_xic_pt(run, argc, argv, setting);
}


