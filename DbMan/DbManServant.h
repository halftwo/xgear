#ifndef DbManServant_h_
#define DbManServant_h_

#include "MySQLdb.h"
#include "DBCluster.h"
#include "CallerKind.h"
#include "xic/ServantI.h"
#include "xslib/XEvent.h"
#include "xslib/LruHashMap.h"
#include <memory>


#define DBMAN_CMDS		\
	CMD(sQuery)		\
	CMD(mQuery)		\
	CMD(tableNumber)	\
	CMD(xidName)		\
	CMD(kindInfo)		\
	CMD(kindVersions)	\
	/* END OF CMDS */

class DbManServant: public xic::ServantI, private XMutex
{
	MySQLdb *_db;
	DBClusterPtr _cluster;
	std::string _charset;
	int _max_conn;

	CallerKindMap _writerMap;

	XEvent::DispatcherPtr _dispatcher;

	static xic::MethodTab::PairType _funpairs[];
	static xic::MethodTab _funtab;
public:
	DbManServant(const SettingPtr& setting, const std::string& charset);
	virtual ~DbManServant();

	bool reloadDBSetting();
	void getStat(iobuf_t *ob);
	bool setActive(int sid, bool active);
	DBClusterPtr getCluster() const;

private:
	std::string kind_detail(const std::string& kind, const DBClusterPtr& cluster, DBSetting::KindSetting *ks);
	void check_cluster_thread();
	void reap_writer_thread();

#define CMD(X) XIC_METHOD_DECLARE(X);
	DBMAN_CMDS
#undef CMD
};
typedef XPtr<DbManServant> DbManServantPtr;



#define DBMANCTRL_CMDS		\
	CMD(reloadDBSetting)	\
	CMD(getStat)		\
	CMD(setActive)		\
	CMD(allKinds)		\
	CMD(allServers)		\
	/* END OF CMDS */

class DbManCtrlServant: public xic::ServantI
{
	DbManServantPtr _dbman;
	time_t _start_time;
	time_t _reload_time;

	static xic::MethodTab::PairType _funpairs[];
	static xic::MethodTab _funtab;

public:
	DbManCtrlServant(const DbManServantPtr& dbman);
	virtual ~DbManCtrlServant();

private:
#define CMD(X) XIC_METHOD_DECLARE(X);
	DBMANCTRL_CMDS
#undef CMD
};
typedef XPtr<DbManCtrlServant> DbManCtrlServantPtr;


#endif
