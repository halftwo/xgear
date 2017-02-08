#include "DbManServant.h"
#include "QueryJob.h"
#include "dlog/dlog.h"
#include "xslib/vbs.h"
#include "xslib/XThread.h"
#include "xslib/Setting.h"
#include "xslib/xlog.h"
#include "xslib/strbuf.h"
#include "xslib/iobuf.h"
#include "xslib/md5.h"
#include "xslib/ScopeGuard.h"
#include "xslib/rdtsc.h"
#include <unistd.h>
#include <set>

xic::MethodTab::PairType DbManServant::_funpairs[] = {
#define CMD(X)	{ #X, XIC_METHOD_CAST(DbManServant, X) },
	DBMAN_CMDS
#undef CMD
};

xic::MethodTab DbManServant::_funtab(_funpairs, XS_ARRCOUNT(_funpairs));

DbManServant::DbManServant(const SettingPtr& setting, const std::string& charset)
	: ServantI(&_funtab), _charset(charset), _max_conn(0)
{
	std::string dbsetting = setting->wantString("DbMan.DbSetting");
	_max_conn = setting->getInt("DbMan.DbConnection.MaxSize", 6);
	_db = new MySQLdb(dbsetting.c_str(), NULL, 0, "");
	DBSettingPtr st(new DBSetting(_db, false));
	_dispatcher = XEvent::Dispatcher::create();
	_dispatcher->setThreadPool(4, 16, 256*1024);
	_dispatcher->start();
	_cluster.reset( new DBCluster(_dispatcher, st, _charset, _max_conn));

	xref_inc();
	XThread::create(this, &DbManServant::check_cluster_thread);
	XThread::create(this, &DbManServant::reap_writer_thread);
	xref_dec_only();
}

DbManServant::~DbManServant()
{
	delete _db;
}

DBClusterPtr DbManServant::getCluster() const
{
	Lock lock(*this);
	return _cluster;
}

void DbManServant::reap_writer_thread()
{
	for (; true; sleep(1))
	{
		time_t now = time(NULL);
		_writerMap.reap(now);
	}
}

void DbManServant::check_cluster_thread()
{
	time_t last_load_time = 0;
	time_t last_ping_time = 0;

	for (; true; sleep(1))
	try 
	{
		time_t now = time(NULL);
		DBClusterPtr cluster;
		if (now >= last_load_time + 29)
		{
			last_load_time = now;
			cluster = getCluster();

			std::string cluster_rev = cluster->dbsetting()->revision();
			dlog("DB_REV", "%s", cluster_rev.c_str());

			DBSettingPtr st(cluster->dbsetting()->check_load_new_revision(_db));

			if (st)
			{
				DBClusterPtr new_cluster(new DBCluster(_dispatcher, st, _charset, _max_conn));
				if (new_cluster)
				{
					dlog("RELOAD_SUCCESS", "auto");
					cluster_rev = new_cluster->dbsetting()->revision();

					{
						Lock sync(*this);
						_cluster = new_cluster;
					}
					cluster->shutdown();
				}
				else
				{
					dlog("RELOAD_FAIL", "auto");
				}
			}
		}

		if (now >= last_ping_time + 11)
		{
			last_ping_time = now;
			_db->ping();
		}
	}
	catch (std::exception& ex)
	{
		dlog("DB_ERROR", "exception=%s", ex.what());
	}
}

XIC_METHOD(DbManServant, sQuery)
{
	DBClusterPtr cluster = getCluster();
	DBJobPtr job(new SQueryJob(current, quest, cluster, _writerMap));
	cluster->assignJob(job);
	return xic::ASYNC_ANSWER;
}

XIC_METHOD(DbManServant, mQuery)
{
	DBClusterPtr cluster = getCluster();
	DBJobPtr job(new MQueryJob(current, quest, cluster, _writerMap));
	cluster->assignJob(job);
	return xic::ASYNC_ANSWER;
}

XIC_METHOD(DbManServant, tableNumber)
{
	xic::VDict args = quest->args();
	std::string kind = make_string(args.wantXstr("kind"));
	DBClusterPtr cluster = getCluster();
	DBSetting::KindSetting *ks = cluster->dbsetting()->getKind(kind);
	if (!ks)
		throw XERROR_FMT(XError, "No such kind(%s) in DBSetting", kind.c_str());

	xic::AnswerWriter aw;
	aw.param("tableNumber", ks->table_num);
	return aw;
}

XIC_METHOD(DbManServant, xidName)
{
	xic::VDict args = quest->args();
	std::string kind = make_string(args.wantXstr("kind"));
	DBClusterPtr cluster = getCluster();
	DBSetting::KindSetting *ks = cluster->dbsetting()->getKind(kind);
	if (!ks)
		throw XERROR_FMT(XError, "No such kind(%s) in DBSetting", kind.c_str());

	xic::AnswerWriter aw;
	aw.param("xidName", ks->id_field);
	return aw;
}

std::string DbManServant::kind_detail(const std::string& kind, const DBClusterPtr& cluster, DBSetting::KindSetting *ks)
{
	std::ostringstream ss;
	iobuf_t tmp_ob = make_iobuf(ss, NULL, 0);
	iobuf_t *ob = &tmp_ob; 

	std::set<int> sset;

	iobuf_printf(ob, "       kind = %s\n", kind.c_str());
	iobuf_printf(ob, "    enabled = %s\n", ks->enable ? "true" : "false");
	iobuf_printf(ob, "tablePrefix = %s\n", ks->table_prefix);
	iobuf_printf(ob, "tableNumber = %d\n", ks->table_num);
	iobuf_printf(ob, "    xidName = %s\n", ks->id_field);
	iobuf_printf(ob, "    version = %d\n", ks->version);
	iobuf_printf(ob, "\n");

	iobuf_printf(ob, " No.   Database and sid...\n");
	iobuf_printf(ob, "----- ----------------------\n");
	for (size_t i = 0; i < ks->tables.size(); ++i)
	{
		DBSetting::TableSetting& ts = ks->tables[i];
		sset.insert(ts.sid);
		iobuf_printf(ob, "%4zu   %s %d", i, ts.db_name, ts.sid);
		DBSetting::ServerSetting* ss = cluster->dbsetting()->getServer(ts.sid);
		if (ss)
		{
			for (size_t k = 0; k < ss->slaves.size(); ++k)
			{
				iobuf_printf(ob, " %d", ss->slaves[k]);
				sset.insert(ss->slaves[k]);
			}
		}
		iobuf_printf(ob, "\n");
	}
	iobuf_printf(ob, "----- ----------------------\n");
	iobuf_printf(ob, "\n");

	iobuf_printf(ob, " sid     master   host:port\n"); 
	iobuf_printf(ob, "------- -------- -----------------------\n");
	for (std::set<int>::iterator iter = sset.begin(); iter != sset.end(); ++iter)
	{
		int sid = *iter;
		DBSetting::ServerSetting* ss = cluster->dbsetting()->getServer(sid);
		if (!ss)
			iobuf_printf(ob, "?%5d\n", sid);
		else
		{
			iobuf_printf(ob, "%c%5d   %6d   %s:%d\n", ss->active ? ' ' : '*', sid,
				ss->master_sid, ss->host, ss->port);
		}
	}
	iobuf_printf(ob, "------- -------- -----------------------\n");
	return ss.str();
}

XIC_METHOD(DbManServant, kindInfo)
{
	xic::VDict args = quest->args();
	std::string kind = make_string(args.wantXstr("kind"));
	std::vector<xstr_t> facets = args.wantXstrVector("facets");
	DBClusterPtr cluster = getCluster();
	DBSetting::KindSetting *ks = cluster->dbsetting()->getKind(kind);
	if (!ks)
		throw XERROR_FMT(XError, "No such kind(%s) in DBSetting", kind.c_str());

	xic::AnswerWriter aw;
	aw.param("kind", kind);
	for (size_t i = 0; i < facets.size(); ++i)
	{
		const xstr_t& facet = facets[i];
		if (xstr_equal_cstr(&facet, "version"))
		{
			aw.param("version", ks->version);
		}
		else if (xstr_equal_cstr(&facet, "tableNumber"))
		{
			aw.param("tableNumber", ks->table_num);
		}
		else if (xstr_equal_cstr(&facet, "xidName"))
		{
			aw.param("xidName", ks->id_field);
		}
		else if (xstr_equal_cstr(&facet, "detail"))
		{
			aw.param("detail", kind_detail(kind, cluster, ks));
		}
	}

	return aw;
}

XIC_METHOD(DbManServant, kindVersions)
{
	xic::VDict args = quest->args();
	std::vector<xstr_t> kinds = args.wantXstrVector("kinds");
	DBClusterPtr cluster = getCluster();

	xic::AnswerWriter aw;
	xic::VDictWriter dw = aw.paramVDict("kindVersions");
	for (size_t i = 0; i < kinds.size(); ++i)
	{
		std::string kind = make_string(kinds[i]);
		DBSetting::KindSetting *ks = cluster->dbsetting()->getKind(kind);
		int version = ks ? ks->version : -1;
		dw.kv(kind, version);
	}
	return aw;
}

bool DbManServant::reloadDBSetting()
{
	try
	{
		DBSettingPtr st(new DBSetting(_db, true));
		DBClusterPtr cluster(new DBCluster(_dispatcher, st, _charset, _max_conn));
		if (cluster)
		{
			dlog("RELOAD_SUCCESS", "manual");
			{
				Lock sync(*this);
				_cluster.swap(cluster);
			}
			cluster->shutdown();
		}
		else
		{
			dlog("RELOAD_FAIL", "manual");
		}
	}
	catch (std::exception& ex)
	{
		dlog("DB_ERROR", "realodDBSeting() failed, exception=%s", ex.what());
		return false;
	}
	return true;
}

void DbManServant::getStat(iobuf_t *ob)
{
	DBClusterPtr cluster = getCluster();
	cluster->getStat(ob);
}

bool DbManServant::setActive(int sid, bool active)
{
	DBClusterPtr cluster = getCluster();
	return cluster->setActive(sid, active);
}


//
// DbManCtrlServant methods
//

xic::MethodTab::PairType DbManCtrlServant::_funpairs[] = {
#define CMD(X)	{ #X, XIC_METHOD_CAST(DbManCtrlServant, X) },
	DBMANCTRL_CMDS
#undef CMD
};

xic::MethodTab DbManCtrlServant::_funtab(_funpairs, XS_ARRCOUNT(_funpairs));


DbManCtrlServant::DbManCtrlServant(const DbManServantPtr& dbman)
	: ServantI(&_funtab), _dbman(dbman)
{
	_start_time = time(NULL);
	_reload_time = 0;
}

DbManCtrlServant::~DbManCtrlServant()
{
}

XIC_METHOD(DbManCtrlServant, reloadDBSetting)
{
	bool ok = _dbman->reloadDBSetting();
	if (ok)
		_reload_time = time(NULL);

	return xic::AnswerWriter()("ok", ok);
}

XIC_METHOD(DbManCtrlServant, getStat)
{
	std::ostringstream ss;
	iobuf_t ob = make_iobuf(ss, NULL, 0);

	struct tm tm;
	char time_str[32];

	localtime_r(&_start_time, &tm);
	strftime(time_str, sizeof(time_str), "%Y%m%d-%H%M%S", &tm);
	iobuf_printf(&ob, "START_TIME %s\n", time_str);

	if (_reload_time)
	{
		localtime_r(&_reload_time, &tm);
		strftime(time_str, sizeof(time_str), "%Y%m%d-%H%M%S", &tm);
	}
	else
		time_str[0] = 0;
	iobuf_printf(&ob, "MANUAL_RELOAD_TIME %s\n", time_str);

	_dbman->getStat(&ob);

	iobuf_finish(&ob);
	return xic::AnswerWriter()("stat", ss.str());
}

XIC_METHOD(DbManCtrlServant, setActive)
{
	xic::VDict args = quest->args();
	int sid = args.wantInt("sid");
	bool active = args.wantBool("active");

	bool ok = _dbman->setActive(sid, active);
	return xic::AnswerWriter()("ok", ok);
}

XIC_METHOD(DbManCtrlServant, allKinds)
{
	DBClusterPtr cluster = _dbman->getCluster();
	std::ostringstream ss;
	iobuf_t ob = make_iobuf(ss, NULL, 0);
	cluster->dbsetting()->getAllKinds(&ob);

	xic::AnswerWriter aw;
	aw.param("allKinds", ss.str());
	return aw;
}

XIC_METHOD(DbManCtrlServant, allServers)
{
	DBClusterPtr cluster = _dbman->getCluster();
	std::ostringstream ss;
	iobuf_t ob = make_iobuf(ss, NULL, 0);
	cluster->dbsetting()->getAllServers(&ob);

	xic::AnswerWriter aw;
	aw.param("allServers", ss.str());
	return aw;
}

