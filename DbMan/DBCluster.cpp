#include "DBCluster.h"
#include "dlog/dlog.h"
#include "xslib/divmod.h"
#include "xslib/ScopeGuard.h"
#include <sys/time.h>
#include <list>

#define MASTER_QUEUE_SIZE	256
#define SLAVE_QUEUE_SIZE	64
#define DEFAULT_POOL_SIZE	4
#define MAX_POOL_SIZE		256
#define RECON_INTERVAL		(5*1000)
#define PING_INTERVAL		(20*1000)


DBConnection::DBConnection(DBTeam* team, const DBSetting::ServerSetting *ss, const std::string& charset, int slave_no)
	: _dbteam(team), _slave_no(slave_no)
{
	_db = new MySQLdb(ss->host, ss->user, ss->passwd, NULL, ss->port, NULL, CLIENT_MULTI_STATEMENTS, charset);
	_active_time = 0;
}

DBConnection::~DBConnection()
{
	delete _db;
}

void DBConnection::shutdown(const XEvent::DispatcherPtr& dispatcher)
{
	dispatcher->removeTask(this);
}

bool DBConnection::connect()
{
	try {
		_db->connect();
		return true;
	}
	catch (std::exception& ex)
	{
		dlog("EXCEPT", "%s", ex.what());
		return false;
	}
}

int DBConnection::try_connect()
{
	if (connect())
	{
		_dbteam->releaseConnection(DBConnectionPtr(this));
		_dbteam->work(DBJobPtr(), master());
		return 0;
	}
	else
	{
		_dbteam->discardJobs();
		return _dbteam->isShutdown() ? 0 : RECON_INTERVAL;
	}
}

void DBConnection::event_on_task(const XEvent::DispatcherPtr& dispatcher)
{
	if (!_dbteam->removeConnectionNotLast(DBConnectionPtr(this)))
	{
		try {
			_db->ping();
			dispatcher->addTask(this, PING_INTERVAL);
		}
		catch (std::exception& ex)
		{
			dlog("EXCEPTION", "%s", ex.what());
		}
	}
}

DBTeam::DBTeam(const XEvent::DispatcherPtr& dispatcher, const DBSettingPtr& dbsetting,
		const DBSetting::ServerSetting *ss, const std::string& charset, int max_con)
	: _dispatcher(dispatcher), _dbsetting(dbsetting), _masterPool(ss, -1), _charset(charset)
{
	_masterPool.ss = ss;
	_last_slave = 0;
	_shutdown = false;
	_max4all = max_con <= 0 ? DEFAULT_POOL_SIZE
		: max_con < MAX_POOL_SIZE ? max_con : MAX_POOL_SIZE;
	_max4read = (_max4all * 3 + 3) / 4;

	for (size_t i = 0; i < ss->slaves.size(); ++i)
	{
		DBSetting::ServerSetting *s = dbsetting->getServer(ss->slaves[i]);
		ConPool pool(s, i);
		_slavePools.push_back(pool);
	}
}

void DBTeam::shutdown()
{
	Lock lock(*this);
	_shutdown = true;

	ConPool *p = &_masterPool;
	for (size_t k = 0; k < p->cons.size(); ++k)
	{	
		const DBConnectionPtr& con = p->cons[k];
		con->shutdown(_dispatcher);
	}
	p->cons.clear();

	for (size_t i = 0; i < _slavePools.size(); ++i)
	{
		p = &_slavePools[i];
		for (size_t k = 0; k < p->cons.size(); ++k)
		{	
			const DBConnectionPtr& con = p->cons[k];
			con->shutdown(_dispatcher);
		}
		p->cons.clear();
	}
}

DBTeam::ConPool::ConPool(const DBSetting::ServerSetting *s, int slave)
	: ss(s), slave_no(slave)
{
	this->num_busy = 0;
	this->error = false;
	this->error_time = 0;
}

void DBTeam::discardJobs()
{
	std::deque<DBJobPtr> jobs;

	{
		Lock lock(*this);
		if (_masterPool.error)
			_masterQueue.swap(jobs);

		bool slave_error = true;
		for (std::vector<ConPool>::iterator iter =  _slavePools.begin(); iter != _slavePools.end(); ++iter)
		{
			if (!iter->error)
			{
				slave_error = false;
				break;
			}
		}

		if (slave_error && _masterPool.error)
		{
			if (jobs.size())
			{
				jobs.insert(jobs.end(), _slaveQueue.begin(), _slaveQueue.end());	
				_slaveQueue.clear();
			}
			else
			{
				_slaveQueue.swap(jobs);
			}
		}
	}

	if (jobs.size())
	{
		XERROR_VAR_FMT(XError, ex, "NO CONNECTION");
		for (std::deque<DBJobPtr>::iterator iter = jobs.begin(); iter != jobs.end(); ++iter)
		{
			(*iter)->cancel(ex);
		}
	}
}


DBConnectionPtr DBTeam::acquireConnection(bool master)
{
	DBConnectionPtr con;

	Lock lock(*this);
	size_t num_slave = _slavePools.size();
	if (!master && num_slave) 
	{
		size_t which = _last_slave;
		ConPool *pool = NULL;
		for (size_t i = 0; i < num_slave; ++i, ++which)
		{
			if (which >= num_slave)
				which = 0;

			ConPool *p = &_slavePools[which];
			if (!p->error && p->ss->active && p->num_busy < _max4all)
			{
				if (pool == NULL || pool->num_busy > p->num_busy)
				{
					pool = p;
					_last_slave = which;
					if (p->num_busy == 0)
						break;
				}
			}
		}

		if (pool)
		{
			if (!pool->cons.empty())
			{
				con = pool->cons.back();	
				pool->cons.pop_back();	
			}
			else
			{
				con.reset(new DBConnection(this, pool->ss, _charset, pool->slave_no));
			}
			++pool->num_busy;
			return con;
		}
	}

	ConPool *p = &_masterPool;
	if (!p->error && p->ss->active && (p->num_busy < _max4read || (master && p->num_busy < _max4all)))
	{
		if (!p->cons.empty())
		{
			con = p->cons.back();	
			p->cons.pop_back();	
		}
		else
		{
			con.reset(new DBConnection(this, p->ss, _charset, p->slave_no));
		}
		++p->num_busy;
	}
	return con;
}

void DBTeam::releaseConnection(const DBConnectionPtr& con)
{
	Lock lock(*this);
	ConPool& pool = con->master() ? _masterPool : _slavePools[con->slave_no()];
	--pool.num_busy;
	assert(pool.num_busy >= 0);
	if (con->ok())
	{
		pool.error = false;
		con->setActiveTime(time(NULL));
		if (_shutdown)
		{
			_dispatcher->removeTask(con);
		}
		else
		{
			pool.cons.push_back(con);
			_dispatcher->replaceTask(con, PING_INTERVAL);
		}
	}
	else
	{
		_dispatcher->removeTask(con);
		if (pool.num_busy <= 0 && pool.cons.empty() && !_shutdown)
		{
			pool.error = true;
			++pool.num_busy;
			_dispatcher->replaceTask(XEvent::TaskHandler::create(con, &DBConnection::try_connect), 0);
		}
	}
}

bool DBTeam::removeConnectionNotLast(const DBConnectionPtr& con)
{
	Lock lock(*this);
	ConPool& pool = con->master() ? _masterPool : _slavePools[con->slave_no()];
	if (_shutdown || pool.num_busy > 0 || pool.cons.size() > 1)
	{
		for (std::vector<DBConnectionPtr>::iterator iter = pool.cons.begin(); iter != pool.cons.end(); ++iter)
		{
			if (*iter == con)
			{
				pool.cons.erase(iter);
				break;
			}
		}
		return true;
	}
	return false;
}

DBJobPtr DBTeam::_fetchJob(bool master)
{
	DBJobPtr job;

	Lock lock(*this);
	if (master && !_masterQueue.empty())
	{
		job = _masterQueue.front();
		_masterQueue.pop_front();
		return job;
	}

	if (!_slaveQueue.empty())
	{
		if (!master || _masterPool.num_busy < _max4read)
		{
			job = _slaveQueue.front();
			_slaveQueue.pop_front();
		}
	}

	return job;
}

void DBTeam::work(const DBJobPtr& job/*0*/, bool master)
{
	DBConnectionPtr con = acquireConnection(master);
	if (!con || !con->ok())
	{
		if (job)
		{
			Lock lock(*this);
			if (master)
			{
				if (_masterQueue.size() < MASTER_QUEUE_SIZE)
					_masterQueue.push_back(job);
				else
					job->cancel(XERROR_FMT(XError, "BUSY TO WRITE, group=%d kind=%.*s",
						_masterPool.ss->sid, XSTR_P(&job->kind())));
			}
			else
			{
				if (_slaveQueue.size() < SLAVE_QUEUE_SIZE)
					_slaveQueue.push_back(job);
				else
					job->cancel(XERROR_FMT(XError, "BUSY TO READ, group=%d kind=%.*s",
						_masterPool.ss->sid, XSTR_P(&job->kind())));
			}
		}

		if (!con)
		{
			discardJobs();
			return;
		}
	}

	ON_BLOCK_EXIT_OBJ(*this, &DBTeam::releaseConnection, con);

	if (con->ok() && job)
	{
		job->doit(con);
	}
	else
	{
		con->connect();
	}

	while (con->ok())
	{
		DBJobPtr j = _fetchJob(con->master());
		if (!j)
			break;

		j->doit(con);
	}
}

void DBTeam::_do_getStat(ConPool& cp, iobuf_t *ob, int master_sid)
{
	struct tm tm;
	char time_str[32];

	if (cp.error_time)
	{
		localtime_r(&cp.error_time, &tm);
		strftime(time_str, sizeof(time_str), "%Y%m%d-%H%M%S", &tm);
	}
	else
	{
		time_str[0] = 0;
	}

	iobuf_printf(ob, "%cG%d:%d %s:%d %s %s", master_sid == cp.ss->sid ? '+' : '-',
		master_sid, cp.ss->sid, cp.ss->host, cp.ss->port,
		cp.ss->active ? "active" : "inactive", cp.error ? "error" : "normal");

	iobuf_putc(ob, '\n');
/*
	iobuf_printf(ob, " query=%lld,%d avg_msec=%d.%03d,%d.%03d busy=%d/%zu num_err=%d err_time=%s\n",
		(long long)cp.num_query, cp.m5_query, cp.avg_usec/1000, cp.avg_usec%1000, cp.a5_usec/1000, cp.a5_usec%1000,
		cp.num_busy, cp.pool.size(), cp.num_error, time_str);
*/
}

void DBTeam::getStat(iobuf_t *ob)
{
	Lock lock(*this);
	iobuf_printf(ob, "BEGIN Group_%d\n", _masterPool.ss->sid);

	_do_getStat(_masterPool, ob, _masterPool.ss->sid);
	for (size_t i = 0; i < _slavePools.size(); ++i)
	{
		_do_getStat(_slavePools[i], ob, _masterPool.ss->sid);
	}

	iobuf_printf(ob, "END Group_%d\n", _masterPool.ss->sid);
}



DBCluster::DBCluster(const XEvent::DispatcherPtr& dispatcher, const DBSettingPtr& dbsetting, const std::string& charset, int max_con)
	: _dispatcher(dispatcher), _dbsetting(dbsetting), _charset(charset)
{
	_max_con = max_con <= 0 ? DEFAULT_POOL_SIZE
		: max_con < MAX_POOL_SIZE ? max_con : MAX_POOL_SIZE;
}

DBCluster::~DBCluster()
{
	shutdown();
}

void DBCluster::assignJob(const DBJobPtr& job)
{
	int sid = job->sid();
	DBTeamPtr team;
	{
		Lock lock(*this);
		DBMap::iterator iter = _dbmap.find(sid);
		if (iter == _dbmap.end())
		{
			DBSetting::ServerSetting *ss = _dbsetting->getServer(sid);
			if (!ss)
				throw XERROR_FMT(XLogicError, "Can't find ServerSetting for sid(%d)", sid);

			team.reset(new DBTeam(_dispatcher, _dbsetting, ss, _charset, _max_con));
			_dbmap[sid] = team;
		}
		else
		{
			team = iter->second;
		}
	}

	bool master = job->master();
	team->work(job, master);
}

void DBCluster::getStat(iobuf_t *ob)
{
	std::vector<DBTeamPtr> ts;

	{
		Lock lock(*this);
		ts.reserve(_dbmap.size());
		for (DBMap::iterator iter = _dbmap.begin(); iter != _dbmap.end(); ++iter)
		{
			ts.push_back(iter->second);
		}
	}

	struct tm tm;
	char time_str[32];
	time_t t = _dbsetting->load_time();
	localtime_r(&t, &tm);
	strftime(time_str, sizeof(time_str), "%Y%m%d-%H%M%S", &tm);
	iobuf_printf(ob, "SETTING_LOAD_TIME %s\nSETTING_REVISION %s\nMAX_DB_CONNECTION %d\n",
		time_str, _dbsetting->revision().c_str(), _max_con);

	for (std::vector<DBTeamPtr>::iterator iter = ts.begin(); iter != ts.end(); ++iter)
	{
		(*iter)->getStat(ob);
	}
}

bool DBCluster::setActive(int sid, bool active)
{
	DBSetting::ServerSetting *ss = _dbsetting->getServer(sid);
	if (ss == NULL)
		return false;
	ss->active = active;
	return true;
}

void DBCluster::shutdown()
{
	std::vector<DBTeamPtr> ts;
	{
		Lock lock(*this);
		ts.reserve(_dbmap.size());
		for (DBMap::iterator iter = _dbmap.begin(); iter != _dbmap.end(); ++iter)
		{
			ts.push_back(iter->second);
		}
	}

	for (std::vector<DBTeamPtr>::iterator iter = ts.begin(); iter != ts.end(); ++iter)
	{
		(*iter)->shutdown();
	}
}

