#include "IdManServant.h"
#include "SQLResult.h"
#include "dlog/dlog.h"
#include "xslib/xbase57.h"
#include "xslib/rdtsc.h"
#include "xslib/xformat.h"
#include "xslib/ScopeGuard.h"
#include "xslib/uuid.h"
#include "xslib/hashint.h"
#include "xslib/XThread.h"
#include <unistd.h>

#define TIMESHIFT	20
#define TIMEVOLUME	(1 << TIMESHIFT)
#define EXTRA_MAX	2048

#define SYNC_INTERVAL	1


xic::MethodTab::PairType IdManServant::_funpairs[] = {
#define CMD(X)	{ #X, XIC_METHOD_CAST(IdManServant, X) },
	IDMAN_CMDS
#undef CMD
};

xic::MethodTab IdManServant::_funtab(_funpairs, XS_ARRCOUNT(_funpairs));


struct item {
	bool timed;
	int64_t last_id;
	int64_t sync_id;
};

static inline int get_random(int max)	// [0, max)
{
	return (int)(max * (random() / (RAND_MAX + 1.0)));
}

IdManServant::IdManServant(const SettingPtr& setting)
	: ServantI(&_funtab)
{
	std::string dbsetting = setting->wantString("IdMan.DbSetting");
	if (dbsetting.empty())
		throw XERROR_MSG(XError, "IdMan.DbSetting not set in the configuration");

	_db = new MySQLdb(dbsetting.c_str(), NULL, 0, "binary");
	ENFORCE(_db);
	_db->connect();
	_hd = hdict_create(65536, 0, sizeof(struct item));
	_stopped = false;

	init_from_db();

	srandom(time(NULL));
	_timer = XTimer::create();
	set_timeid_highid();
	for (size_t i = 0; i < XS_ARRCOUNT(_extras); ++i)
	{
		_extras[i] = get_random(EXTRA_MAX);
	}
	_extra_idx = 0;

	xref_inc();
	XThread::create(this, &IdManServant::db_sync_thread);
	XThread::create(this, &IdManServant::timeid_thread);
	xref_dec_only();

}

IdManServant::~IdManServant()
{
	_timer->cancel();
	delete _db;
	hdict_destroy(_hd);
}


void IdManServant::setSelfProxy(const xic::ProxyPtr& prx)
{
	{
		Lock lock(*this);
		_selfPrx = prx;
	}

	char sql[1024];
	int len = xfmt_snprintf(mysql_xfmt, sql, sizeof(sql),
		"replace into variable_setting(name, value) values('idman', '%p{>SQL<}')", prx->str().c_str());
	MYSQL_RES *res = _db->query(sql, len);
	ON_BLOCK_EXIT(mysql_free_result, res);
}

void *IdManServant::get_item(const ::std::string& kind, XMutex::Lock& lock)
{
	void *item = hdict_find(_hd, kind.c_str());
	if (!item)
	{
		item = _from_db(kind, lock);
	}
	return item;
}

void *IdManServant::_from_db(const ::std::string& kind, XMutex::Lock& lock)
{
	std::set<std::string>::iterator it = _nonkinds.find(kind);
	bool no_kind = (it != _nonkinds.end());

	lock.release();

	if (!no_kind)
	try {
		char sql[1024];
		int len = xfmt_snprintf(mysql_xfmt, sql, sizeof(sql),
			"select * from idman_generator where binary kind='%p{>SQL<}'", kind.c_str());
		MYSQL_RES *res = _db->query(sql, len);
		ON_BLOCK_EXIT(mysql_free_result, res);

		MYSQL_ROW row = mysql_fetch_row(res);
		if (row != NULL)
		{
			bool timed = false;
			int64_t last_id = 0;
			size_t num_fields = mysql_num_fields(res);
			MYSQL_FIELD *fields = mysql_fetch_fields(res);
			for (size_t i = 0; i < num_fields; i++)
			{
				if (strcasecmp(fields[i].name, "timed") == 0)
				{
					timed = atoi(row[i]);
				}
				else if (strcasecmp(fields[i].name, "last_id") == 0)
				{
					last_id = atoll(row[i]);
				}
			}

			lock.acquire();
			struct item *item = (struct item *)hdict_insert(_hd, kind.c_str(), NULL);
			if (item)
			{
				item->timed = timed;
				item->last_id = last_id;
			}
			else
			{
				item = (struct item *)hdict_find(_hd, kind.c_str());
				if (item)
				{
					item->timed = timed;
					/* NB: Do NOT use value of last_id in DB. */
				}
			}
			lock.release();

			return item;
		}
		else
		{
			lock.acquire();
			_nonkinds.insert(kind);
			lock.release();
		}
	}
	catch (MySQLdb::Error& e)
	{
		dlog("ERROR", "%s", e.what());
	}

	return NULL;
}

bool IdManServant::reset_id(const ::std::string& kind, int64_t id)
{
	Lock lock(*this);
	if (_stopped)
		return false;

	struct item *item = (struct item *)get_item(kind, lock);
	if (!item)
		return false;

	item->last_id = id;
	return true;
}

int64_t IdManServant::get_newid(const ::std::string& kind, int num, bool timed)
{
	Lock lock(*this);
	if (_stopped)
		return 0;

	struct item *item = (struct item *)get_item(kind, lock);
	if (!item)
		return 0;

	int64_t newid;
	if (timed && item->timed)
	{
		if (item->last_id < _timeid)
		{
			item->last_id = _timeid + get_random(TIMEVOLUME);
		}
		else if (item->last_id < _highid)
		{
			// make the interval of ids more random
			item->last_id += _extras[_extra_idx];
			if (++_extra_idx >= EXTRA_NUM)
			{
				_extra_idx = 0;
			}
		}
	}
	++item->last_id;
	newid = item->last_id;

	if (num > 1)
		item->last_id += (num - 1);

	return newid;
}

int64_t IdManServant::get_lastid(const ::std::string& kind)
{
	Lock lock(*this);

	struct item *item = (struct item *)get_item(kind, lock);
	if (!item)
		return -1;

	return item->last_id;
}

XIC_METHOD(IdManServant, newId)
{
	xic::VDict args = quest->args();
	std::string kind = make_string(args.wantXstr("kind"));
	int64_t id = get_newid(kind, 1, false);
	return xic::AnswerWriter()("id", id);
}

XIC_METHOD(IdManServant, newTimeId)
{
	xic::VDict args = quest->args();
	std::string kind = make_string(args.wantXstr("kind"));
	int64_t id = get_newid(kind, 1, true);
	return xic::AnswerWriter()("id", id);
}


XIC_METHOD(IdManServant, lastId)
{
	xic::VDict args = quest->args();
	std::string kind = make_string(args.wantXstr("kind"));
	int64_t id = get_lastid(kind);
	return xic::AnswerWriter()("id", id);
}

bool IdManServant::clear_token(const ::std::string& token)
{
	Lock lock(*this);
	return _ctrls.erase(token);
}

class ClearTokenTask: public XTimerTask
{
	IdManServantPtr _idman;
	std::string _token;
public:
	ClearTokenTask(IdManServant* idman, const std::string& token)
		: _idman(idman), _token(token)
	{}

	virtual void runTimerTask(const XTimerPtr& timer)
	{
		_idman->clear_token(_token);
	}
};

XIC_METHOD(IdManServant, ctrl)
{
	xic::VDict args = quest->args();
	int t = args.getInt("time", -1);

	uuid_t uuid;
	uuid_generate_random(uuid);

	char token[XBASE57_ENCODED_LEN(sizeof(uuid)) + 1];
	xbase57_encode(token, uuid, sizeof(uuid));

	if (t <= 0 || t > 60)
	{
		t = 60;
		{
			Lock lock(*this);
			_ctrls.insert(token);
		}

		XTimerTaskPtr task(new ClearTokenTask(this, token));
		_timer->addTask(task, t * 1000);
	}

	return xic::AnswerWriter()("token", token);
}

static std::string _get_ctrl(const xic::QuestPtr& quest)
{
	xic::VDict ctx = quest->context();
	return make_string(ctx.getXstr("CTRL"));
}

XIC_METHOD(IdManServant, reload)
{
	std::string ctrl = _get_ctrl(quest);
	xic::VDict args = quest->args();
	std::string kind = make_string(args.wantXstr("kind"));

	bool ok = false;
	if (!ctrl.empty() && clear_token(ctrl))
	{
		Lock lock(*this);
		ok = _from_db(kind, lock);
	}

	return xic::AnswerWriter()("ok", ok);
}

XIC_METHOD(IdManServant, reserveId)
{
	std::string ctrl = _get_ctrl(quest);
	xic::VDict args = quest->args();
	std::string kind = make_string(args.wantXstr("kind"));
	int num = args.wantInt("num");

	int64_t id = 0;
	if (!ctrl.empty() && clear_token(ctrl))
	{
		id = get_newid(kind, num, false);
	}

	return xic::AnswerWriter()("id", id);
}

XIC_METHOD(IdManServant, resetId)
{
	std::string ctrl = _get_ctrl(quest);
	xic::VDict args = quest->args();
	std::string kind = make_string(args.wantXstr("kind"));
	int64_t id = args.wantInt("id");

	bool ok = false;
	if (!ctrl.empty() && clear_token(ctrl))
	{
		ok = reset_id(kind, id);
	}

	return xic::AnswerWriter()("ok", ok);
}

void IdManServant::set_timeid_highid()
{
	_timeid = (int64_t)time(NULL) << TIMESHIFT;
	_highid = _timeid + TIMEVOLUME + get_random(TIMEVOLUME);
}

void IdManServant::timeid_thread()
{
	for (int n = 0; !_stopped; sleep(1), ++n)
	{
		Lock lock(*this);
		set_timeid_highid();
		_extras[get_random(EXTRA_NUM)] = get_random(EXTRA_MAX);
	}
}

void IdManServant::db_sync_thread()
{
	for (int idle = 0; !_stopped; sleep(SYNC_INTERVAL), ++idle)
	{
		try {
			if (idle > 15)
			{
				idle = 0;
				_db->ping();
			}
		}
		catch (MySQLdb::Error& ex)
		{
			dlog("ERROR", "%s", ex.what());
		}

		{
			Lock lock(*this);
			if (!_nonkinds.empty())
				_nonkinds.clear();
		}

		sync_all_to_db();
	}
}

void IdManServant::stop()
{
	_stopped = true;
}


void IdManServant::init_from_db()
{
	char sql[1024];
	int len;
	len = snprintf(sql, sizeof(sql), "select * from idman_generator");
	MYSQL_RES *res = _db->query(sql, len);
	ON_BLOCK_EXIT(mysql_free_result, res);

	size_t num_fields = mysql_num_fields(res);
	MYSQL_FIELD *fields = mysql_fetch_fields(res);
	int kind_idx = -1;
	int timed_idx = -1;
	int last_id_idx = -1;
	for (size_t i = 0; i < num_fields; ++i)
	{
		if (strcasecmp(fields[i].name, "kind") == 0)
		{
			kind_idx = i;
		}
		else if (strcasecmp(fields[i].name, "timed") == 0)
		{
			timed_idx = i;
		}
		else if (strcasecmp(fields[i].name, "last_id") == 0)
		{
			last_id_idx = i;
		}
	}

	if (kind_idx < 0 || last_id_idx < 0)
	{
		dlog("FATAL", "no 'kind' or 'last_id' field in table idman_generator");
		return;
	}

	MYSQL_ROW row;
	while ((row = mysql_fetch_row(res)) != NULL)
	{
		char *kind = row[kind_idx];
		struct item *item = (struct item *)hdict_insert(_hd, kind, NULL);
		if (item)
		{
			item->timed = timed_idx < 0 ? false : atoi(row[timed_idx]);
			item->last_id = atoll(row[last_id_idx]);
			item->sync_id = item->last_id;
		}
	}
}

std::string IdManServant::get_db_idman()
{
	char sql[1024];
	int len = snprintf(sql, sizeof(sql), "select value from variable_setting where name='idman'");
	MYSQL_RES *res = _db->query(sql, len);
	ON_BLOCK_EXIT(mysql_free_result, res);
	MYSQL_ROW row = mysql_fetch_row(res);
	return row ? row[0] : "";
}


struct Record {
	char *kind;
	int64_t last_id;
};

void IdManServant::sync_all_to_db()
{
	std::string idsrv;
	std::vector<Record> rs;

	{
		Lock lock(*this);
		if (!_selfPrx)
			return;

		idsrv = _selfPrx->str();
		char *kind;
		struct item *item;
		hdict_iter_t iter;
		hdict_iter_init(_hd, &iter);
		while ((item = (struct item *)hdict_iter_next(&iter, (void **)(void *)&kind)) != NULL)
		{
			if (item->last_id != item->sync_id)
			{
				Record record;
				record.kind = kind;
				record.last_id = item->last_id;
				rs.push_back(record);

				item->sync_id = item->last_id;
			}
		}
	}

	try
	{
		if (get_db_idman() != idsrv)
		{
			dlog("FATAL", "Another IdMan server is running! Exit.");
			stop();
		}

		for (std::vector<Record>::iterator it = rs.begin(); it != rs.end(); ++it)
		{
			char sql[1024];
			int len = xfmt_snprintf(mysql_xfmt, sql, sizeof(sql),
				"update idman_generator set last_id='%jd' where binary kind='%p{>SQL<}'",
				(intmax_t)it->last_id, it->kind);
			_db->query(sql, len);
			dlog("LAST_ID", "%s=%jd", it->kind, (intmax_t)it->last_id);
		}
	}
	catch (MySQLdb::Error& e)
	{
		dlog("ERROR", "%s", e.what());
	}
}


