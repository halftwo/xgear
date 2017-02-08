#ifndef QueryJob_h_
#define QueryJob_h_

#include "CallerKind.h"
#include "MySQLdb.h"
#include "DBCluster.h"
#include "xic/Engine.h"
#include <vector>


class QueryJob: public DBJob
{
protected:
	xic::WaiterPtr _waiter;
	xic::QuestPtr _quest;
	int64_t _hintId;
	bool _master;
	bool _convert;
	bool _preserve_null;

	int _table_num;
	int _table_no;
	int _sid;
	const char *_db_name;

	xstr_t _kind;
	xstr_t _query;

public:
	virtual int sid() const 			{ return _sid; }
	virtual bool master() const			{ return _master; }
	virtual const xstr_t& kind() const		{ return _kind; }
	virtual void cancel(const std::exception& ex);

	bool check_and_rewrite_sql(ostk_t *ostk, const xstr_t& sql, const xstr_t& kind, const char *table_prefix);
};


class SQueryJob: public QueryJob
{
	xstr_t _sql;
public:
	SQueryJob(const xic::Current& current, const xic::QuestPtr& quest, const DBClusterPtr& cluster, CallerKindMap& writerMap);
	virtual void doit(const DBConnectionPtr& con);
};


class MQueryJob: public QueryJob, public MySQLdb::ResultCB
{
	std::vector<xstr_t> _kinds;
	std::vector<xstr_t> _sqls;
	int _error_sql;
	xic::AnswerPtr _answer;
public:
	MQueryJob(const xic::Current& current, const xic::QuestPtr& quest, const DBClusterPtr& cluster, CallerKindMap& writerMap);
	virtual void doit(const DBConnectionPtr& con);
	virtual void process(MYSQL *mysql);
	int error_sql() const { return _error_sql; }
};


#endif
