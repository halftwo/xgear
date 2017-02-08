#ifndef MySQLdb_h_
#define MySQLdb_h_

#include "xslib/XLock.h"
#include "xslib/XError.h"
#include "xslib/Enforce.h"
#include <mysql/mysql.h>
#include <mysql/errmsg.h>
#include <stdint.h>
#include <string>

class MySQLdb: private XRecMutex
{
public:
	class Error: public XError
	{
	public:
		XE_DEFAULT_METHODS_EX(XError, Error, "MySQLdb::Error")
		virtual ~Error() throw() {}
		virtual const char *what() const throw();

		std::string sql;
	};

	struct ResultExt {
		int64_t affected_rows;
		int64_t insert_id;
		char *info;
	};

	class ResultCB
	{
	public:
		virtual void process(MYSQL *mysql) = 0;
		virtual int error_sql() const { return -1; }
		virtual ~ResultCB() {}
	};

	enum 
	{
		DEFAULT_CONNECT_TIMEOUT = 5,
	};

public:
	/* mysql://user:passwd@host:port/db */
	MySQLdb(const char *uri, const char *unix_socket, unsigned long client_flag, const std::string& charset);
	MySQLdb(const char *host, const char *user, const char *passwd, const char *db, unsigned int port, const char *unix_socket, unsigned long client_flag, const std::string& charset);
	~MySQLdb();

	void set_connect_timeout(unsigned int seconds) { _connect_timeout = seconds; }

	MYSQL *connect();
	MYSQL *reconnect();
	void close();
	void ping();

	MYSQL_RES *query(const char *sql, size_t size=0, const char *db_name=NULL, ResultExt *re=NULL);
	void query(ResultCB *cb, const char *sql, size_t size=0, const char *db_name=NULL);

	MYSQL *mysql()	{ return _mysql; }

private:
	void fault();
	uint64_t _fault_tsc;

	MYSQL *_mysql;

	char *_host;
	char *_user;
	char *_passwd;
	char *_db;
	int _db_size;
	unsigned int _port;
	char *_unix_socket;
	unsigned long _flag;
	unsigned int _connect_timeout;
	std::string _charset;
};


#endif

