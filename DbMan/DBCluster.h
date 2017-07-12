#ifndef DBCluster_h_
#define DBCluster_h_

#include "xslib/XRefCount.h"
#include "xslib/XLock.h"
#include "xslib/iobuf.h"
#include "xslib/rdtsc.h"
#include "xslib/XEvent.h"
#include "MySQLdb.h"
#include "DBSetting.h"
#include <deque>

class DBJob;
class DBConnection;
class DBTeam;
typedef XPtr<DBJob> DBJobPtr;
typedef XPtr<DBConnection> DBConnectionPtr;
typedef XPtr<DBTeam> DBTeamPtr;


class DBJob: virtual public XRefCount
{
public:
	virtual int sid() const					= 0;
	virtual bool master() const				= 0;
	virtual const xstr_t& kind() const			= 0;
	virtual void doit(const DBConnectionPtr& con)		= 0;
	virtual void cancel(const std::exception& ex)		= 0;
};


class DBConnection: public XEvent::TaskHandler
{
public:
	DBConnection(DBTeam* team, const DBSetting::ServerSetting *ss, const std::string& charset, int slave_no);
	virtual ~DBConnection();

	MySQLdb *db() const				{ return _db; }
	bool ok() const					{ return _db->mysql(); }
	bool master() const				{ return _slave_no < 0; }
	int slave_no() const 				{ return _slave_no; }

	void setActiveTime(time_t t)			{ _active_time = t; }

	void shutdown(const XEvent::DispatcherPtr& dispatcher);
	bool connect();
	int try_connect();

	virtual void event_on_task(const XEvent::DispatcherPtr& dispatcher);
private:
	DBTeamPtr _dbteam;
	MySQLdb *_db;
	int _slave_no;
	time_t _active_time;
};


class DBTeam: virtual public XRefCount, private XMutex
{
public:
	DBTeam(const XEvent::DispatcherPtr& dispatcher, const DBSettingPtr& dbsetting,
		const DBSetting::ServerSetting *ss, const std::string& charset, int max_con);

	void work(const DBJobPtr& job, bool master);
	void discardJobs();
	void getStat(iobuf_t *ob);
	void shutdown();

	bool isShutdown() const				{ return _shutdown; }
	const DBSettingPtr& dbsetting() const		{ return _dbsetting; }

	bool removeConnectionNotLast(const DBConnectionPtr& con);
	DBConnectionPtr acquireConnection(bool master);
	void releaseConnection(const DBConnectionPtr& con);

private:
	struct ConPool
	{
		const DBSetting::ServerSetting *ss;
		std::vector<DBConnectionPtr> cons;
		int num_busy;
		int slave_no;
		bool error;
		time_t error_time;

		ConPool(const DBSetting::ServerSetting *ss, int slave);
	};

	DBJobPtr _fetchJob(bool master);
	void _do_check(ConPool &cp, time_t t);
	void _do_getStat(ConPool& pool, iobuf_t *ob, int master_sid);

private:
	XEvent::DispatcherPtr _dispatcher;
	DBSettingPtr _dbsetting;
	ConPool _masterPool;
	std::vector<ConPool> _slavePools;
	std::deque<DBJobPtr> _masterQueue;
	std::deque<DBJobPtr> _slaveQueue;
	int _last_slave;
	int _max4all;
	int _max4read;
	bool _shutdown;
	std::string _charset;
};


class DBCluster: virtual public XRefCount, private XRecMutex
{
public:
	DBCluster(const XEvent::DispatcherPtr& dispatcher, const DBSettingPtr& st, const std::string& charset, int max_conn=0);
	virtual ~DBCluster();

	void assignJob(const DBJobPtr& job);
	void getStat(iobuf_t *ob);

	bool setActive(int sid, bool active);
	void shutdown();

	const DBSettingPtr& dbsetting() const 		{ return _dbsetting; }

private:
	typedef std::map<int, DBTeamPtr> DBMap;
	XEvent::DispatcherPtr _dispatcher;
	DBMap _dbmap;
	DBSettingPtr _dbsetting;
	int _max_con;
	std::string _charset;
};
typedef XPtr<DBCluster> DBClusterPtr;

#endif
