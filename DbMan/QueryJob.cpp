#include "QueryJob.h"
#include "type4vbs.h"
#include "xslib/divmod.h"
#include "xslib/strbuf.h"
#include "xslib/ScopeGuard.h"
#include "xslib/xlog.h"
#include <set>


static ssize_t _seek_after(const xstr_t *str, ssize_t start, const xstr_t *after)
{
	ssize_t t = xstr_case_find(str, start, after);
	if (t >= 0)
	{
		do {
			ssize_t n = t + after->len;
			if ((t == 0 || isspace(str->data[t-1])) && (n < str->len && isspace(str->data[n])))
			{
				++n;
				while (n < str->len && isspace(str->data[n]))
					++n;
				return n;
			}
			else
			{
				while (n < str->len && !isspace(str->data[n]))
					++n;
			}
			t = xstr_case_find(str, n, after);
		} while (t >= 0);
	}
	return -1;
}

static bool _find_table_name(const xstr_t *str, ssize_t pos, const xstr_t *after, xstr_t *tname)
{
	ssize_t t = _seek_after(str, pos, after);
	if (t >= 0)
	{
		do {
			if (isalpha(str->data[t]))
			{
				const unsigned char *start = &str->data[t];
				const unsigned char *end = start + 1;
				while (end < xstr_end(str) && !isspace(*end) && *end != '(')
					++end;
				xstr_init(tname, (unsigned char *)start, end - start);
				return true;
			}
			else if (str->data[t] == '`')
			{
				const unsigned char *start = &str->data[t+1];
				const unsigned char *end = (unsigned char *)memchr(start, '`', xstr_end(str) - start);
				if (end)
				{
					xstr_init(tname, (unsigned char *)start, end - start);
					return true;
				}
			}
			t = _seek_after(str, t, after);
		} while (t >= 0);
	}
	return false;
}

static inline bool start_with_word(const xstr_t *str, const xstr_t *word)
{
	return xstr_case_start_with(str, word) && (word->len < str->len) && isspace(str->data[word->len]);
}

/*
select * from t where cond
insert into t(x,y,z) values(1,2,3)
replace into t(x,y,z) values(1,2,3)
update t set x=1,y=2,z=3 where cond
delete from t wehre cond
desc t
*/

static bool is_allowed_sql(const xstr_t& the_sql, xstr_t *tname)
{
	static xstr_t delete_xs = XSTR_CONST("delete");
	static xstr_t desc_xs = XSTR_CONST("desc");
	static xstr_t describe_xs = XSTR_CONST("describe");
	static xstr_t explain_select_xs = XSTR_CONST("explain select");
	static xstr_t insert_xs = XSTR_CONST("insert");
	static xstr_t replace_xs = XSTR_CONST("replace");
	static xstr_t select_xs = XSTR_CONST("select");
	static xstr_t update_xs = XSTR_CONST("update");
	static xstr_t from_xs = XSTR_CONST("from");
	static xstr_t into_xs = XSTR_CONST("into");
	static xstr_t where_xs = XSTR_CONST("where");
	bool has_tname = false;
	bool allowed = false;
	xstr_t tmp = the_sql;
	xstr_t *sql = &tmp;

	xstr_trim(sql);
	char first_char = sql->len ? tolower(sql->data[0]) : 0;

	switch (first_char)
	{
	case 'd':	// delete, desc, describe
		if (start_with_word(sql, &delete_xs))
		{
			has_tname = _find_table_name(sql, delete_xs.len, &from_xs, tname);
			if (has_tname && _seek_after(sql, tname->data + tname->len - sql->data, &where_xs) > 0)
			{
				allowed = true;
			}
		}
		else if (start_with_word(sql, &desc_xs))
		{
			has_tname = _find_table_name(sql, 0, &desc_xs, tname);
			allowed = true;
		}
		else if (start_with_word(sql, &describe_xs))
		{
			has_tname = _find_table_name(sql, 0, &describe_xs, tname);
			allowed = true;
		}
		break;

	case 'e':	// explain select
		if (start_with_word(sql, &explain_select_xs))
		{
			has_tname = _find_table_name(sql, explain_select_xs.len, &from_xs, tname);
			allowed = true;
		}
		break;

	case 'i':	// insert
		if (start_with_word(sql, &insert_xs))
		{
			has_tname = _find_table_name(sql, insert_xs.len, &into_xs, tname);
			allowed = true;
		}
		break;

	case 'r':	// replace
		if (start_with_word(sql, &replace_xs))
		{
			has_tname = _find_table_name(sql, replace_xs.len, &into_xs, tname);
			allowed = true;
		}
		break;

	case 's':	// select
		if (start_with_word(sql, &select_xs))
		{
			has_tname = _find_table_name(sql, select_xs.len, &from_xs, tname);
			allowed = true;
		}
		break;

	case 'u':	// update
		if (start_with_word(sql, &update_xs))
		{
			has_tname = _find_table_name(sql, 0, &update_xs, tname);
			if (has_tname && _seek_after(sql, tname->data + tname->len - sql->data, &where_xs) > 0)
			{
				allowed = true;
			}
		}
		break;

	default:
		allowed = false;
		break;
	}

	if (!has_tname)
		*tname = xstr_null;

	return allowed;
}

static inline xstr_t get_table_name_from_sql(const xstr_t& sql)
{
	xstr_t tname;
	if (!is_allowed_sql(sql, &tname))
		throw XERROR_FMT(XError, "Not allowed SQL: %.*s", XSTR_P(&sql));
	return tname;
}

static inline bool sql_may_write(const xstr_t& sql)
{
	static xstr_t select_xs = XSTR_CONST("select");
	return !start_with_word(&sql, &select_xs);
}

bool QueryJob::check_and_rewrite_sql(ostk_t *ostk, const xstr_t& sql, const xstr_t& kind, const char *table_prefix)
{
	xstr_t tname;
	if (!is_allowed_sql(sql, &tname))
	{
		throw XERROR_FMT(XError, "Not allowed SQL: %.*s", XSTR_P(&sql));
	}
	else if (tname.len > 0)
	{
		if (xstr_case_equal(&tname, &kind))
		{
			ostk_object_grow(ostk, sql.data, tname.data - sql.data);
			if (table_prefix[0])
			{
				ostk_object_puts(ostk, table_prefix);
			}
			else
			{
				ostk_object_grow(ostk, kind.data, kind.len);
			}
			if (_table_num > 1)
			{
				ostk_object_printf(ostk, "_%d", _table_no);
			}
			ostk_object_grow(ostk, xstr_end(&tname), xstr_end(&sql) - xstr_end(&tname));
			return true;
		}
	}

	return false;
}


void QueryJob::cancel(const std::exception& ex)
{
	_waiter->response(ex);
}

SQueryJob::SQueryJob(const xic::Current& current, const xic::QuestPtr& quest, const DBClusterPtr& cluster, CallerKindMap& writerMap)
{
	_query = xstr_null;
	_waiter = current.asynchronous();
	_quest = quest;

	xic::VDict args = _quest->args();
	_kind = args.getXstr("kind");
	_sql = args.wantBlob("sql");
	xstr_trim(&_sql);
	if (!_sql.len)
		throw XERROR_FMT(XError, "EMPTY SQL");

	_hintId = args.wantInt("hintId");
	_convert = args.getBool("convert");
	_preserve_null = args.getBool("null");

	xic::VDict ctx = _quest->context();
	// NB. Parameter master is deprecated, it is supported for backward compatibility.
	// New programs should use context MASTER.
	_master = ctx.getBool("MASTER") || args.getBool("master");

	if (!_kind.len)
	{
		_kind = get_table_name_from_sql(_sql);
		if (_kind.len == 0)
			throw XERROR_FMT(XError, "table name can't be get from sql: %.*s", XSTR_P(&_sql));
	}

	DBSetting::KindSetting *ks = cluster->dbsetting()->getKind(make_string(_kind));
	if (!ks)
		throw XERROR_FMT(XError, "No such kind(%.*s) in DBSetting", XSTR_P(&_kind));
	else if (!ks->enable)
		throw XERROR_FMT(XError, "Disabled kind(%.*s)", XSTR_P(&_kind));

	_table_num = ks->table_num;
	_table_no = _table_num > 1 ? floored_mod(_hintId, _table_num) : 0;
	_sid = ks->tables[_table_no].sid;
	_db_name = ks->tables[_table_no].db_name;

	bool maywrite = sql_may_write(_sql);
	if (maywrite)
	{
		_master = true;
	}

	xstr_t caller = quest->context().getXstr("CALLER");
	if (caller.len)
	{
		time_t now = time(NULL);
		const std::string& con = current.con->info();
		CallerKindMd5 ck(con, caller, _kind);
		if (maywrite)
		{
			writerMap.replace(now, ck);
		}
		else
		{
			if (writerMap.find(now, ck))
				_master = true;
		}
	}

	ostk_t *ostk = _quest->ostk();
	if (check_and_rewrite_sql(ostk, _sql, _kind, ks->table_prefix))
	{
		_query = ostk_object_finish_xstr(ostk);
	}
	else
	{
		_query = _sql;
	}

	if (!maywrite && _master)
	{
		xlog(XLOG_INFO, "reading sql goes to master db, sql=%.*s", XSTR_P(&_sql));
	}
}

void SQueryJob::doit(const DBConnectionPtr& con)
{
	MySQLdb *db = con->db();
	try 
	{
		MySQLdb::ResultExt myr;
		MYSQL_RES *res = db->query((char *)_query.data, _query.len, _db_name, &myr);
		ON_BLOCK_EXIT(mysql_free_result, res);
		ON_BLOCK_EXIT(free, myr.info);

		xic::AnswerWriter aw;
		aw.param("converted", _convert);
		aw.param("affectedRowNumber", myr.affected_rows);
		if (myr.insert_id)
		{
			aw.param("insertId", myr.insert_id);
		}
		if (myr.info)
		{
			aw.param("info", myr.info);
		}

		if (res)
		{
			MYSQL_FIELD *fds = mysql_fetch_fields(res);
			int num_fds = mysql_num_fields(res);
			int num_rows = mysql_num_rows(res);
			uint8_t *types = (uint8_t *)(num_rows > 0 ? alloca(num_fds * sizeof(uint8_t)) : NULL);
			xic::VListWriter lw = aw.paramVList("fields");
			for (int i = 0; i < num_fds; ++i)
			{
				lw.v(fds[i].name);
				if (types)
					types[i] = type4vbs(&fds[i]);
			}

			lw = aw.paramVList("rows");
			MYSQL_ROW row;
			size_t sum = 0;
			for (int n = 0; (row = mysql_fetch_row(res)) != NULL; ++n)
			{
				if (n > 0)
					aw.suggest_block_size(sum * num_rows / (2 * n));
				unsigned long *lengths = mysql_fetch_lengths(res);
				xic::VListWriter llw = lw.vlist();
				for (int i = 0; i < num_fds; ++i)
				{
					sum += lengths[i];
					xstr_t xs = XSTR_INIT((unsigned char *)row[i], (ssize_t)lengths[i]);
					if (!row[i] && _preserve_null)
					{
						llw.vnull();
					}
					else if (types[i] == VBS_BLOB)
					{
						llw.vblob(xs);
					}
					else if (_convert)
					{
						if (types[i] == VBS_INTEGER)
						{
							xstr_t end;
							intmax_t v = xstr_to_integer(&xs, &end, 0);
							if (end.len)
								llw.vstring(xs);
							else
								llw.v(v);
						}
						else if (types[i] == VBS_FLOATING)
						{
							xstr_t end;
							double v = xstr_to_double(&xs, &end);
							if (end.len)
								llw.vstring(xs);
							else
								llw.v(v);
						}
						else if (types[i] == VBS_DECIMAL)
						{
							xstr_t end;
							decimal64_t v;
							int rc = decimal64_from_xstr(&v, &xs, &end);
							if (rc < 0 || end.len)
								llw.vstring(xs);
							else
								llw.v(v);
						}
						else
						{
							llw.vstring(xs);
						}
					}
					else
					{
						llw.vstring(xs);
					}
				}
			}
		}
		_waiter->response(aw);
	}
	catch (std::exception& ex)
	{
		_waiter->response(ex);
	}
}


MQueryJob::MQueryJob(const xic::Current& current, const xic::QuestPtr& quest, const DBClusterPtr& cluster, CallerKindMap& writerMap)
{
	_query = xstr_null;
	_waiter = current.asynchronous();
	_quest = quest;
	xic::VDict args = _quest->args();
	args.getXstrSeq("kinds", _kinds);
	if (_kinds.empty())
	{
		_kind = args.getXstr("kind");
		if (_kind.len)
		{
			_kinds.push_back(_kind);
		}
	}
	else
	{
		_kind = _kinds[0];
	}

	_quest->args().wantBlobSeq("sqls", _sqls);
	size_t size = _sqls.size();
	if (size == 0)
		throw XERROR_MSG(xic::ParameterDataException, "no sql given");
	else if (size > 100)
		throw XERROR_MSG(xic::ParameterDataException, "too many sqls, the number of sqls should be not greater than 100");

	_hintId = args.wantInt("hintId");
	_convert = args.getBool("convert");
	_preserve_null = args.getBool("null");

	xic::VDict ctx = _quest->context();
	// NB. Parameter master is deprecated, it is supported for backward compatibility.
	// New programs should use context MASTER.
	_master = ctx.getBool("MASTER") || args.getBool("master");

	xstr_t caller = quest->context().getXstr("CALLER");
	const std::string& con = current.con->info();
	time_t now = time(NULL);

	ostk_t *ostk = _quest->ostk();
	ostk_object_puts(ostk, "BEGIN; ");
	
	DBSetting::KindSetting *last_ks = NULL;
	xstr_t last_kind = xstr_null;
	for (size_t i = 0; i < _sqls.size(); ++i)
	{
		xstr_t sql = _sqls[i];
		xstr_trim(&sql);
		if (!sql.len)
		{
			throw XERROR_FMT(XError, "EMPTY SQL");
		}

		xstr_t kind = xstr_null;
		if (i < _kinds.size())
		{
			kind = _kinds[i];
		}
		else
		{
			kind = get_table_name_from_sql(sql);
			if (_kind.len == 0)
				_kind = kind;
		}

		DBSetting::KindSetting *ks = NULL;
		if (kind.len)
		{
			ks = cluster->dbsetting()->getKind(make_string(kind));
			if (!ks)
				throw XERROR_FMT(XError, "No such kind(%.*s) in DBSetting", XSTR_P(&kind));
			else if (!ks->enable)
				throw XERROR_FMT(XError, "Disabled kind(%.*s)", XSTR_P(&kind));
		}
		else if (last_ks)
		{
			ks = last_ks;
			kind = last_kind;
		}
		else
		{
			throw XERROR_FMT(XError, "No kind specified");
		}

		if (i == 0)
		{
			_table_num = ks->table_num;
			_table_no = _table_num > 1 ? floored_mod(_hintId, _table_num) : 0;
			_sid = ks->tables[_table_no].sid;
			_db_name = ks->tables[_table_no].db_name;
		}
		else if (last_ks != ks)
		{
			if (_table_num != ks->table_num
				|| _sid != ks->tables[_table_no].sid
				|| strcmp(_db_name, ks->tables[_table_no].db_name) != 0)
			{
				throw XERROR_FMT(XError, "SQLs in one transaction must have same table_number, same db server and same db name");
			}
		}

		last_ks = ks;
		last_kind = kind;

		bool maywrite = sql_may_write(sql);
		if (maywrite)
		{
			_master = true;
		}

		if (caller.len)
		{
			CallerKindMd5 ck(con, caller, kind);
			if (maywrite)
			{
				writerMap.replace(now, ck);
			}
			else
			{
				if (writerMap.find(now, ck))
					_master = true;
			}
		}

		if (!maywrite && _master)
		{
			xlog(XLOG_INFO, "reading sql goes to master db, sql=%.*s", XSTR_P(&sql));
		}

		if (!check_and_rewrite_sql(ostk, sql, kind, ks->table_prefix))
		{
			ostk_object_grow(ostk, sql.data, sql.len);
		}

		if (sql.data[sql.len - 1] != ';')
			ostk_object_puts(ostk, "; ");
	}
	ostk_object_puts(ostk, "COMMIT");
	_query = ostk_object_finish_xstr(ostk);
}

void MQueryJob::doit(const DBConnectionPtr& con)
{
	MySQLdb *db = con->db();
	try {
		db->query(this, (char *)_query.data, _query.len, _db_name);
		ENFORCE(_answer);
		_waiter->response(_answer);
	}
	catch (std::exception& ex)
	{
		_waiter->response(ex);
	}
}

void MQueryJob::process(MYSQL *mysql)
{
	_error_sql = 0;
	// for BEGIN;
	ENFORCE(mysql_field_count(mysql) == 0 && mysql_store_result(mysql) == NULL);

	xic::AnswerWriter aw;
	aw.param("converted", _convert);
	xic::VListWriter lw = aw.paramVList("results");
	for (size_t i = 0; i < _sqls.size(); ++i)
	{
		++_error_sql;
		ENFORCE(mysql_next_result(mysql) == 0);

		MYSQL_RES *res = mysql_field_count(mysql) ? mysql_store_result(mysql) : NULL;
		ON_BLOCK_EXIT(mysql_free_result, res);

		xic::VDictWriter dw = lw.vdict();
		dw.kv("affectedRowNumber", mysql_affected_rows(mysql));
		int64_t insertId = mysql_insert_id(mysql);
		if (insertId)
		{
			dw.kv("insertId", insertId);
		}
		const char *info = mysql_info(mysql);
		if (info)
		{
			dw.kv("info", info);
		}
		if (res)
		{
			MYSQL_FIELD *fds = mysql_fetch_fields(res);
			int num_fds = mysql_num_fields(res);
			int num_rows = mysql_num_rows(res);
			xic::VListWriter llw = dw.kvlist("fields");
			uint8_t *types = (uint8_t *)(num_rows > 0 ? alloca(num_fds * sizeof(uint8_t)) : NULL);
			for (int i = 0; i < num_fds; ++i)
			{
				llw.v(fds[i].name);
				if (types)
					types[i] = type4vbs(&fds[i]);
			}

			llw = dw.kvlist("rows");
			MYSQL_ROW row;
			size_t sum = 0;
			for (int n = 0; ((row = mysql_fetch_row(res)) != NULL); ++n)
			{
				if (n > 0)
					aw.suggest_block_size(sum * num_rows / (2 * n));
				unsigned long *lengths = mysql_fetch_lengths(res);
				xic::VListWriter l3w = llw.vlist();
				for (int i = 0; i < num_fds; ++i)
				{
					sum += lengths[i];
					xstr_t xs = XSTR_INIT((unsigned char *)row[i], (ssize_t)lengths[i]);
					if (!row[i] && _preserve_null)
					{
						l3w.vnull();
					}
					else if (types[i] == VBS_BLOB)
					{
						l3w.vblob(xs);
					}
					else if (_convert)
					{
						if (types[i] == VBS_INTEGER)
						{
							xstr_t end;
							intmax_t v = xstr_to_integer(&xs, &end, 0);
							if (end.len)
								l3w.vstring(xs);
							else
								l3w.v(v);
						}
						else if (types[i] == VBS_FLOATING)
						{
							xstr_t end;
							double v = xstr_to_double(&xs, &end);
							if (end.len)
								l3w.vstring(xs);
							else
								l3w.v(v);
						}
						else if (types[i] == VBS_DECIMAL)
						{
							xstr_t end;
							decimal64_t v;
							int rc = decimal64_from_xstr(&v, &xs, &end);
							if (rc < 0 || end.len)
								l3w.vstring(xs);
							else
								l3w.v(v);
						}
						else
						{
							l3w.vstring(xs);
						}
					}
					else
					{
						l3w.vstring(xs);
					}
				}
			}
		}
	}

	// for COMMIT;
	++_error_sql;
	ENFORCE(mysql_next_result(mysql) == 0);
	_error_sql = -1;

	_answer = aw.take();
}

