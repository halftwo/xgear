#include "DBSetting.h"
#include "dlog/dlog.h"
#include "xslib/ScopeGuard.h"
#include <unistd.h>
#include <errno.h>
#include <string.h>

static std::string get_revision(MySQLdb *db)
{
	char sql[1024];
	int len = snprintf(sql, sizeof(sql), "select value from variable_setting where name='revision'");
	MYSQL_RES *res = db->query(sql, len);
	ON_BLOCK_EXIT(mysql_free_result, res);
	MYSQL_ROW row = mysql_fetch_row(res);
	return row ? row[0] : "";
}

DBSetting* DBSetting::check_load_new_revision(MySQLdb *db)
{
	std::string new_revision = get_revision(db);
	if (_revision != new_revision)
	{
		dlog("DB_REV_NEW", "%s", new_revision.c_str());
		return new DBSetting(db, true);
	}
	return NULL;
}

struct table_entry {
	char *kind;
	int no;
	int sid;
	char *db_name;
};

static int compare_table_entry(const void *p1, const void *p2)
{
	struct table_entry *e1 = (struct table_entry *)p1;
	struct table_entry *e2 = (struct table_entry *)p2;
	int r = strcasecmp(e1->kind, e2->kind);
	if (r == 0)
	{
		r =  e1->no < e2->no ? -1
			: e1->no == e2->no ? 0
			: 1;
	}
	return r;
}

void DBSetting::load(MySQLdb *db, bool strict)
{
	char sql[1024];
	int len;
	MYSQL_RES *res;
	MYSQL_ROW row;

	if (_servers.size() > 0 || _kinds.size() > 0)
	{
		// Alreadly loaded.
		return;
	}

	_load_time = time(NULL);
	_revision = get_revision(db);

	len = snprintf(sql, sizeof(sql), "select sid, master_sid, host, port, user, passwd, active from server_setting");
	res = db->query(sql, len);

	{
		ON_BLOCK_EXIT(mysql_free_result, res);
		while ((row = mysql_fetch_row(res)) != NULL)
		{
			ServerSetting *s = new(_mem) ServerSetting;
			s->sid = atoi(row[0]);
			s->master_sid = atoi(row[1]);
			s->host = ostk_strdup(_mem, row[2]);
			s->port = atoi(row[3]);
			s->user = ostk_strdup(_mem, row[4]);
			s->passwd = ostk_strdup(_mem, row[5]);
			s->active = atoi(row[6]);
			_servers[s->sid] = s;
		}
	}

	for (std::map<int, ServerSetting*>::iterator it = _servers.begin(); it != _servers.end(); ++it)
	{
		ServerSetting *s = it->second;
		if (s->master_sid)
		{
			std::map<int, ServerSetting*>::iterator master_iter = _servers.find(s->master_sid);
			if (master_iter == _servers.end())
			{
				if (strict)
				{
					throw XERROR_FMT(Error, "No such master server (sid=%d) in server_setting for server (sid=%d)",
						s->master_sid , s->sid);
				}
				else
				{
					dlog("DB_WARNING", "No such master server (sid=%d) in server_setting for server (sid=%d)",
						s->master_sid , s->sid);

					continue;
				}
			}

			ServerSetting *master = master_iter->second;
			if (master->master_sid)
			{
				if (strict)
				{
					throw XERROR_FMT(Error, "Server (sid=%d) is slave of server(%d) but master of server(%d)",
						master->sid, master->master_sid, s->sid);
				}
				else
				{
					dlog("DB_WARNING", "Server (sid=%d) is slave of server(%d) but master of server(%d)",
						master->sid, master->master_sid, s->sid);
				}
			}
			else
			{
				master->slaves.push_back(s->sid);
			}
		}
	}

	// Load _kinds
	len = snprintf(sql, sizeof(sql), "select kind, table_num, table_prefix, id_field, enable, version from kind_setting");
	res = db->query(sql, len);

	{
		ON_BLOCK_EXIT(mysql_free_result, res);
		while ((row = mysql_fetch_row(res)) != NULL)
		{
			KindSetting *ks = new(_mem) KindSetting;
			char *kind = row[0];
			ks->table_num = atoi(row[1]);
			ks->table_prefix = ostk_strdup(_mem, row[2]);
			ks->id_field = ostk_strdup(_mem, row[3]);
			ks->enable = atoi(row[4]);
			ks->version = atoi(row[5]);
			_kinds[kind] = ks;
		}
	}

	// Load tables
	len = snprintf(sql, sizeof(sql), "select `kind`, `no`, `sid`, `db_name` from table_setting");
	res = db->query(sql, len);

	{
		ON_BLOCK_EXIT(mysql_free_result, res);
		size_t num_rows = mysql_num_rows(res);
		struct table_entry *entries = (struct table_entry *)calloc(num_rows, sizeof(*entries));
		ON_BLOCK_EXIT(free, entries);
		size_t num = 0;
		while ((row = mysql_fetch_row(res)) != NULL && num < num_rows)
		{
			entries[num].kind = row[0];
			entries[num].no = atoi(row[1]);
			entries[num].sid = atoi(row[2]);
			entries[num].db_name = row[3];
			++num;
		}

		qsort(entries, num, sizeof(*entries), compare_table_entry);

		char kind[256];
		int expected_no = 0;
		KindSetting *ks = NULL;
		kind[0] = 0;
		for (size_t i = 0; i < num; ++i)
		{
			char *knd = entries[i].kind;
			int no = entries[i].no;
			int sid = entries[i].sid;
			char *db_name = entries[i].db_name;

			if (strcmp(knd, kind) != 0)
			{
				if (ks && expected_no != ks->table_num)
				{
					if (strict)
					{
						throw XERROR_FMT(Error, "Not enough tables (got only %d, should have %d) in table_setting for kind=%s", expected_no, ks->table_num, kind);
					}
					else
					{
						// Do nothing. We will reap invalid kind later just befor function return.
					}
				}

				strncpy(kind, knd, sizeof(kind));
				expected_no = 0;
				std::map<std::string, KindSetting*>::iterator iter = _kinds.find(kind);
				if (iter != _kinds.end())
				{
					ks = iter->second;
				}
				else
				{
					ks = NULL;
					dlog("DB_WARNING", "Kind(%s) found in table_setting but not in kind_setting", kind);
				}
			}

			if (!ks)
				continue;

			if (no != expected_no)
			{
				if (strict)
				{
					throw XERROR_FMT(Error, "Missing table no %d in table_setting for kind=%s", expected_no, kind);
				}
				else
				{
					dlog("DB_WARNING", "Missing table no %d in table_setting for kind=%s", expected_no, kind);
					ks = NULL;
					continue;
				}
			}
			++expected_no;

			TableSetting t;
			ServerSetting *s = getServer(sid);
			if (!s || s->master_sid != 0)
			{
				if (strict)
				{
					throw XERROR_FMT(Error, "No such server (sid=%d) or it's not master for table (kind=%s no=%d) in table_setting", sid, kind, no);
				}
				else
				{
					dlog("DB_WARNING", "No such server (sid=%d) or it's not master for table (kind=%s no=%d) in table_setting", sid, kind, no);
					ks = NULL;
					continue;
				}
			}

			t.sid = sid;
			t.db_name = ostk_strdup(_mem, db_name);
			ks->tables.push_back(t);
		}
	}

	// Reap invalid kinds 
	for (std::map<std::string, KindSetting*>::iterator iter = _kinds.begin(); iter != _kinds.end(); (void)0)
	{
		KindSetting* ks = iter->second;
		if (ks->tables.size() != (size_t)ks->table_num)
		{
			dlog("DB_WARNING", "Kind(%s) has invalid table_setting entries, discarded", iter->first.c_str());
			_kinds.erase(iter++);
		}
		else
		{
			++iter;
		}
	}
}

DBSetting::DBSetting(const std::string& mysql_uri, bool strict)
{
	MySQLdb db(mysql_uri.c_str(), NULL, 0, "");
	_mem = ostk_create(0);
	try {
		load(&db, strict);
	}
	catch (...)
	{
		ostk_destroy(_mem);
		throw;
	}
}

DBSetting::DBSetting(MySQLdb *db, bool strict)
{
	_mem = ostk_create(0);
	try {
		if (db)
			load(db, strict);
	}
	catch (...)
	{
		ostk_destroy(_mem);
		throw;
	}
}

DBSetting::~DBSetting()
{
	for (std::map<int, ServerSetting*>::iterator it = _servers.begin(); it != _servers.end(); ++it)
	{
		it->second->~ServerSetting();
	}

	for (std::map<std::string, KindSetting*>::iterator it = _kinds.begin(); it != _kinds.end(); ++it)
	{
		it->second->~KindSetting();
	}

	ostk_destroy(_mem);
}

DBSetting::KindSetting *DBSetting::getKind(const std::string& kind)
{
	std::map<std::string, KindSetting*>::iterator it = _kinds.find(kind);
	if (it == _kinds.end())
		return NULL;
	return it->second;
}

DBSetting::ServerSetting *DBSetting::getServer(int sid)
{
	std::map<int, ServerSetting*>::iterator it = _servers.find(sid);
	if (it == _servers.end())
		return NULL;
	return it->second;
}

void DBSetting::getAllKinds(iobuf_t *ob)
{
	std::map<std::string, KindSetting*>::iterator iter;
	int len = 0;
	for (iter = _kinds.begin(); iter != _kinds.end(); ++iter)
	{
		int l = iter->first.length();
		if (len < l)
			len = l;
	}

	for (iter = _kinds.begin(); iter != _kinds.end(); ++iter)
	{
		KindSetting *ks = iter->second;
		iobuf_printf(ob, "%*s %s %d %s [",
			-len, iter->first.c_str(), ks->id_field, ks->table_num,
			ks->enable ? "enable" : "disable");

		size_t size = ks->tables.size();
		if (size)
		{
			iobuf_printf(ob, "%d", ks->tables[0].sid);
			for (size_t i = 1; i < size; ++i)
				iobuf_printf(ob, ",%d", ks->tables[i].sid);
		}
		iobuf_puts(ob, "]\n");
	}
}

void DBSetting::getAllServers(iobuf_t *ob)
{
	std::map<int, ServerSetting*>::iterator iter;
	for (iter = _servers.begin(); iter != _servers.end(); ++iter)
	{
		ServerSetting *ss = iter->second;
		iobuf_printf(ob, "%d %d %s:%d %s [", ss->sid, ss->master_sid, ss->host, ss->port,
			ss->active ? "active" : "inactive");
		size_t size = ss->slaves.size();

		if (size)
		{
			iobuf_printf(ob, "%d", ss->slaves[0]);
			for (size_t i = 1; i < size; ++i)
				iobuf_printf(ob, ",%d", ss->slaves[i]);
		}
		iobuf_puts(ob, "]\n");
	}
}

