#ifndef IdManServant_h_
#define IdManServant_h_

#include "xic/ServantI.h"
#include "xslib/hdict.h"
#include "xslib/XTimer.h"
#include "MySQLdb.h"
#include <stdint.h>
#include <memory>
#include <set>

#define IDMAN_CMDS		\
	CMD(newId)		\
	CMD(newTimeId)		\
	CMD(lastId)		\
	/* The following is for administration only */ \
	CMD(ctrl)		\
	CMD(reload)		\
	CMD(reserveId)		\
	CMD(resetId)		\
	/* END OF CMDS */

class IdManServant: public xic::ServantI, private XMutex
{
	enum { EXTRA_NUM = 64 };
	xic::ProxyPtr _selfPrx;
	hdict_t *_hd;
	MySQLdb *_db;
	std::set<std::string> _nonkinds;
	bool _stopped;
	int64_t _timeid;
	int64_t _highid;
	int _extras[EXTRA_NUM];
	int _extra_idx;
	XTimerPtr _timer;
	std::set<std::string> _ctrls;
	std::string _idsrv;

	static xic::MethodTab::PairType _funpairs[];
	static xic::MethodTab _funtab;
public:
	IdManServant(const SettingPtr& setting);
	virtual ~IdManServant();

	MySQLdb *db() const { return _db; }
	void stop();
	void sync_all_to_db();
	bool clear_token(const ::std::string& token);
	void setSelfProxy(const xic::ProxyPtr& prx);
	std::string get_db_idman();

private:
	void timeid_thread();
	void db_sync_thread();

	void set_timeid_highid();
	void init_from_db();

	void *get_item(const ::std::string& kind, XMutex::Lock& lock);
	void *_from_db(const ::std::string& kind, XMutex::Lock& lock);

	int64_t get_newid(const ::std::string& kind, int num, bool timed);
	bool reset_id(const std::string& kind, int64_t id);
	int64_t get_lastid(const ::std::string& kind);

#define CMD(X) XIC_METHOD_DECLARE(X);
	IDMAN_CMDS
#undef CMD
};
typedef XPtr<IdManServant> IdManServantPtr;

#endif
