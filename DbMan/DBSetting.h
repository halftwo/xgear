#ifndef DBSetting_h_
#define DBSetting_h_

#include "xslib/XRefCount.h"
#include "xslib/XError.h"
#include "xslib/XLock.h"
#include "xslib/ostk.h"
#include "xslib/iobuf.h"
#include "MySQLdb.h"
#include <stdio.h>
#include <map>
#include <vector>

class DBSetting: virtual public XRefCount, private XMutex
{
public:
	XE_(::XError, Error);

	DBSetting(const std::string& mysql_uri, bool strict);
	DBSetting(MySQLdb *db, bool strict);
	~DBSetting();

	DBSetting* check_load_new_revision(MySQLdb *db);
	std::string revision() const 			{ return _revision; }
	time_t load_time() const			{ return _load_time; }

	struct ServerSetting
	{
		int sid;
		int master_sid;
		const char *host;
		int port;
		const char *user;
		const char *passwd;
		bool active;
		std::vector<int> slaves;

		void *operator new(size_t size, ostk_t *mem) 	{ return ostk_alloc(mem, size); }
		void operator delete(void *p, ostk_t *mem) 	{}
	};

	struct TableSetting
	{
		int sid;
		const char *db_name;
	};

	struct KindSetting
	{
		bool enable;
		int version;
		int table_num;
		const char *table_prefix;
		const char *id_field;
		std::vector<TableSetting> tables;

		void *operator new(size_t size, ostk_t *mem) 	{ return ostk_alloc(mem, size); }
		void operator delete(void *p, ostk_t *mem) 	{}
	};

	ServerSetting *getServer(int sid);
	KindSetting *getKind(const std::string& kind);
	void getAllKinds(iobuf_t *ob);
	void getAllServers(iobuf_t *ob);

private:
	void load(MySQLdb *db, bool strict);

private:
	ostk_t *_mem;
	std::map<int, ServerSetting*> _servers;
	std::map<std::string, KindSetting*> _kinds;
	time_t _load_time;
	std::string _revision;
};
typedef XPtr<DBSetting> DBSettingPtr;

#endif

