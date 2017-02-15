#include "MySQLdb.h"
#include "xslib/urlparse.h"
#include "xslib/rdtsc.h"
#include "xslib/xlog.h"
#include "dlog/dlog.h"
#include <stdio.h>
#include <sys/time.h>

#define SLOW_USEC	500000

const char *MySQLdb::Error::what() const throw()
{
	if (_what.length() == 0)
	{
		this->XError::what();
		_what += std::string("\nsql=") + sql;
	}
	return _what.c_str();
}

MySQLdb::MySQLdb(const char *host, const char *user, const char *passwd, const char *db, unsigned int port, const char *unix_socket, unsigned long client_flag, const std::string& charset)
	: _charset(charset)
{
	_host = host ? strdup(host) : NULL;
	_user = user ? strdup(user) : NULL;
	_passwd = passwd ? strdup(passwd) : NULL;
	if (db && db[0])
	{
		_db_size = strlen(db) + 1;
		_db = (char *)malloc(_db_size);
		if (_db)
			memcpy(_db, db, _db_size);
		else
			_db_size = 0;
	}
	else
	{
		_db = NULL;
		_db_size = 0;
	}
	_unix_socket = unix_socket ? strdup(unix_socket) : NULL;
	_port = port;
	_flag = client_flag;
	_mysql = NULL;
	_connect_timeout = DEFAULT_CONNECT_TIMEOUT;
	_fault_tsc = 0;
}

MySQLdb::MySQLdb(const char *uri, const char *unix_socket, unsigned long client_flag, const std::string& charset)
	: _charset(charset)
{
	struct urlpart part;
	xstr_t url = XSTR_C(uri);
	if (urlparse(&url, &part) < 0)
	{
		throw XERROR_MSG(Error, "urlparse() failed");
	}
	_user = part.user.len ? strdup_xstr(&part.user) : NULL;
	_passwd = part.password.len ? strdup_xstr(&part.password) : NULL;
	_host = part.host.len ? strdup_xstr(&part.host) : NULL;
	_port = part.port;
	if (part.path.len > 0 && part.path.data[0] == '/')
		xstr_advance(&part.path, 1);
	if (part.path.len)
	{
		_db = strdup_xstr(&part.path);
		_db_size = _db ? part.path.len + 1 : 0;
	}
	else
	{
		_db = NULL;
		_db_size = 0;
	}
	_unix_socket = unix_socket ? strdup(unix_socket) : NULL;
	_flag = client_flag;
	_mysql = NULL;
	_connect_timeout = DEFAULT_CONNECT_TIMEOUT;
	_fault_tsc = 0;
}

MySQLdb::~MySQLdb()
{
	mysql_close(_mysql);
	free(_host);
	free(_user);
	free(_passwd);
	free(_db);
	free(_unix_socket);
}

MYSQL *MySQLdb::connect()
{
	Lock lock(*this);

	if (_mysql)
		return _mysql;

	_mysql = mysql_init(NULL);
	if (_connect_timeout)
		mysql_options(_mysql, MYSQL_OPT_CONNECT_TIMEOUT, (const char *)&_connect_timeout); 

	if (!mysql_real_connect(_mysql, _host, _user, _passwd, _db, _port, _unix_socket, _flag))
	{
		XERROR_VAR_CODE_FMT(Error, ex, mysql_errno(_mysql), "host=%s:%d, mysql_error=%s", _host ? _host : "", _port, mysql_error(_mysql));
		fault();
		throw ex;
	}

	if (!_charset.empty() && mysql_set_character_set(_mysql, _charset.c_str()))
	{
		XERROR_VAR_CODE_FMT(Error, ex, mysql_errno(_mysql), "host=%s:%d, mysql_error=%s", _host ? _host : "", _port, mysql_error(_mysql));
		fault();
		throw ex;
	}

	return _mysql;
}

MYSQL *MySQLdb::reconnect()
{
	close();
	return connect();
}

void MySQLdb::close()
{
	if (_mysql)
	{
		Lock lock(*this);
		if (_mysql)
		{
			mysql_close(_mysql);
			_mysql = NULL;
		}
	}
}

void MySQLdb::fault()
{
	_fault_tsc = rdtsc();
	close();
}

void MySQLdb::ping()
{
	TryLock trylock(*this);
	if (!trylock.acquired())
		return;

	if (_mysql)
	{
		if (mysql_ping(_mysql) != 0)
		{
			XERROR_VAR_CODE_FMT(Error, ex, mysql_errno(_mysql), "host=%s:%d, mysql_error=%s", _host ? _host : "", _port, mysql_error(_mysql));
			fault();
			throw ex;
		}
	}
	else
		connect();
}

void MySQLdb::query(ResultCB *cb, const char *sql, size_t size, const char *db_name)
{
	int rc;

	Lock lock(*this);

	if (!_mysql)
	{
		uint64_t tsc = rdtsc();
		if (tsc - _fault_tsc < cpu_frequency())
			throw XERROR_FMT(Error, "host=%s:%d, relax for error reconnection interval", _host ? _host : "", _port);

		reconnect();
	}

	uint64_t start_tsc = rdtsc();

	if (db_name && db_name[0] && (!_db || strcasecmp(_db, db_name)))
	{
		if (mysql_select_db(_mysql, db_name) != 0)
		{
			XERROR_VAR_CODE_FMT(Error, ex, mysql_errno(_mysql), "host=%s:%d, mysql_error=%s", _host ? _host : "", _port, mysql_error(_mysql));
			if (mysql_ping(_mysql) != 0)
				fault();
			throw ex;
		}

		int len = strlen(db_name) + 1;
		if (_db_size < len)
		{
			_db = (char *)realloc(_db, len);
			_db_size = _db ? len : 0;
		}

		if (_db)
			memcpy(_db, db_name, len);
	}

	if (size == 0)
	{
		size = strlen(sql);
	}

	rc = mysql_real_query(_mysql, sql, size);

	if (rc)
	{
		XERROR_VAR_CODE_FMT(Error, ex, mysql_errno(_mysql), "host=%s:%d, mysql_error=%s", _host ? _host : "", _port, mysql_error(_mysql));
		ex.sql = std::string(sql, size);
		mysql_rollback(_mysql);
		if (mysql_ping(_mysql) != 0)
			fault();
		throw ex;
	}

	try {
		cb->process(_mysql);
	}
	catch (std::exception& ex_)
	{
		XERROR_VAR_CODE_FMT(Error, ex, mysql_errno(_mysql), "host=%s:%d, mysql_error=%s", _host ? _host : "", _port, mysql_error(_mysql));
		ex.sql = std::string(sql, size);
		mysql_rollback(_mysql);
		throw ex;
	}

	rc = mysql_next_result(_mysql);
	if (rc == 0)
	{
		int n = 1;
		MYSQL_RES *r = mysql_store_result(_mysql);
		mysql_free_result(r);
		while ((rc = mysql_next_result(_mysql)) == 0)
		{
			++n;
			r = mysql_store_result(_mysql);
			mysql_free_result(r);
		}

		dlog("DB_ALERT", "%d UNEXPECTED SQL, #SQL=%.*s", n, (int)size, sql);
		if (rc == -1)
		{
			XERROR_VAR_FMT(Error, ex, "UNEXPECTED SQL");
			ex.sql = std::string(sql, size);
			mysql_rollback(_mysql);
			throw ex;
		}
	}

	if (rc > 0)
	{
		XERROR_VAR_CODE_FMT(Error, ex, mysql_errno(_mysql), "host=%s:%d, mysql_error=%s", _host ? _host : "", _port, mysql_error(_mysql));
		ex.sql = std::string(sql, size);
		mysql_rollback(_mysql);
		if (mysql_ping(_mysql) != 0)
			fault();
		throw ex;
	}

	lock.release();

	int64_t usec = (rdtsc() - start_tsc) * 1000000 / cpu_frequency();
	xlog(1, "DB_QUERY time=%d.%06d db=%s:%u/%s #SQL=%.*s",
		int(usec/1000000), int(usec%1000000),
		(_host ? _host : ""), _port, (db_name ? db_name : _db ? _db : ""), (int)size, sql);

	if (usec > SLOW_USEC)
	{
		dlog("DB_SLOW", "time=%d.%06d db=%s:%u/%s #SQL=%.*s",
			int(usec/1000000), int(usec%1000000),
			(_host ? _host : ""), _port, (db_name ? db_name : _db ? _db : ""), (int)size, sql);
	}
}

MYSQL_RES *MySQLdb::query(const char *sql, size_t size, const char *db_name, ResultExt *re)
{
	int rc;
	MYSQL_RES *res = NULL;

	Lock lock(*this);

	if (!_mysql)
	{
		uint64_t tsc = rdtsc();
		if (tsc - _fault_tsc < cpu_frequency())
			throw XERROR_FMT(Error, "host=%s:%d, relax for error reconnection interval", _host ? _host : "", _port);

		reconnect();
	}

	uint64_t start_tsc = rdtsc();

	if (db_name && db_name[0] && (!_db || strcasecmp(_db, db_name)))
	{
		if (mysql_select_db(_mysql, db_name) != 0)
		{
			XERROR_VAR_CODE_FMT(Error, ex, mysql_errno(_mysql), "host=%s:%d, mysql_error=%s", _host ? _host : "", _port, mysql_error(_mysql));
			if (mysql_ping(_mysql) != 0)
				fault();
			throw ex;
		}

		int len = strlen(db_name) + 1;
		if (_db_size < len)
		{
			_db = (char *)realloc(_db, len);
			_db_size = _db ? len : 0;
		}

		if (_db)
			memcpy(_db, db_name, len);
	}

	if (size == 0)
	{
		size = strlen(sql);
	}

	rc = mysql_real_query(_mysql, sql, size);

	if (rc)
	{
		XERROR_VAR_CODE_FMT(Error, ex, mysql_errno(_mysql), "host=%s:%d, mysql_error=%s", _host ? _host : "", _port, mysql_error(_mysql));
		ex.sql = std::string(sql, size);
		mysql_rollback(_mysql);
		if (mysql_ping(_mysql) != 0)
			fault();
		throw ex;
	}

	if (mysql_field_count(_mysql))
	{
		res = mysql_store_result(_mysql);
		if (!res)
		{
			XERROR_VAR_CODE_FMT(Error, ex, mysql_errno(_mysql), "host=%s:%d, mysql_error=%s", _host ? _host : "", _port, mysql_error(_mysql));
			ex.sql = std::string(sql, size);
			mysql_rollback(_mysql);
			if (mysql_ping(_mysql) != 0)
				fault();
			throw ex;
		}
	}

	if (re)
	{
		re->affected_rows = mysql_affected_rows(_mysql);
		re->insert_id = mysql_insert_id(_mysql);
		const char *info = mysql_info(_mysql);
		re->info = info ? strdup(info) : NULL;
	}

	rc = mysql_next_result(_mysql);
	if (rc == 0)
	{
		int n = 1;
		MYSQL_RES *r = mysql_store_result(_mysql);
		mysql_free_result(r);
		while ((rc = mysql_next_result(_mysql)) == 0)
		{
			++n;
			r = mysql_store_result(_mysql);
			mysql_free_result(r);
		}

		dlog("DB_ALERT", "%d UNEXPECTED SQL, #SQL=%.*s", n, (int)size, sql);
		if (rc == -1)
		{
			XERROR_VAR_FMT(Error, ex, "UNEXPECTED SQL");
			ex.sql = std::string(sql, size);
			mysql_rollback(_mysql);
			throw ex;
		}
	}

	if (rc > 0)
	{
		XERROR_VAR_CODE_FMT(Error, ex, mysql_errno(_mysql), "host=%s:%d, mysql_error=%s", _host ? _host : "", _port, mysql_error(_mysql));
		ex.sql = std::string(sql, size);
		mysql_rollback(_mysql);
		if (mysql_ping(_mysql) != 0)
			fault();
		throw ex;
	}

	lock.release();

	int64_t usec = (rdtsc() - start_tsc) * 1000000 / cpu_frequency();
	xlog(1, "DB_QUERY time=%d.%06d db=%s:%u/%s #SQL=%.*s",
		int(usec/1000000), int(usec%1000000),
		(_host ? _host : ""), _port, (db_name ? db_name : _db ? _db : ""), (int)size, sql);

	if (usec > SLOW_USEC)
	{
		dlog("DB_SLOW", "time=%d.%06d db=%s:%u/%s #SQL=%.*s",
			int(usec/1000000), int(usec%1000000),
			(_host ? _host : ""), _port, (db_name ? db_name : _db ? _db : ""), (int)size, sql);
	}

	return res;
}


