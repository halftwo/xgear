#include "DbManServant.h"

#define DBMAN_VERSION	"20161021.20161021.2150"

static char build_info[] = "$build: DbMan-"DBMAN_VERSION" "__DATE__" "__TIME__" $";

static int run(int argc, char **argv, const xic::EnginePtr& engine)
{
	engine->throb(build_info);
	xic::AdapterPtr adapter = engine->createAdapter();
	DbManServantPtr dbman(new DbManServant(engine->setting(), "binary"));
	DbManCtrlServantPtr ctrl(new DbManCtrlServant(dbman));
	adapter->addServant("DbMan", dbman);
	adapter->addServant("DbManCtrl", ctrl);
	adapter->activate();
	engine->waitForShutdown();
	return 0;
}

int main(int argc, char **argv)
{
	SettingPtr setting = newSetting();
	setting->insert("xic.rlimit.nofile", "8192");
	if (argc > 1 && argv[1][0] != '-')
	{
		setting->load(argv[1]);
	}
	return xic::start_xic_pt(run, argc, argv, setting);
}

