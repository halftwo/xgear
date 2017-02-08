#ifndef MOperation_h_
#define MOperation_h_

#include "xslib/XRefCount.h"
#include "xslib/ostk.h"
#include <sys/uio.h>
#include <vector>

#define FLAG_LZ4_ZIP       0x8000


class MClient;
class MCallback;
class MOperation;
typedef XPtr<MCallback> MCallbackPtr;
typedef XPtr<MOperation> MOperationPtr;


enum MOCategory {
	MOC_NONE,
	MOC_VERSION,
	MOC_STORE,
	MOC_CAS,
	MOC_COUNT,
	MOC_DELETE,
	MOC_GET,
	MOC_GETMULTI,
};

struct MValue
{
	xstr_t key;
	xstr_t value;
	int64_t revision;
	uint32_t flags;
	bool zip;
};


class MCallback: public XRefCount
{
public:
	MCallback(MOCategory category)
		: _category(category)
	{
	}

	virtual xstr_t caller() const					= 0;

	virtual void received(int64_t value)				= 0;

	virtual void received(const MValue values[], size_t n, bool cache,
			void (*cleanup)(void *), void *cleanup_arg)	= 0;

	virtual void completed(bool ok, bool zip=false)			= 0;

protected:
	MOCategory _category;
};

class MOperation: public XRefCount
{
public:
	static void *operator new(size_t size);
	static void operator delete(void *p);

public:
	MOperation(const MCallbackPtr& calback);

	ostk_t* ostk() const			{ return _ostk; }

	MCallbackPtr callback() const 		{ return _callback; }

	struct iovec *get_iovec(int *count);

	MOCategory category() const 		{ return _category; }

	bool appendMValue(const MValue& mv);

	void informCallback();

	void finish(const XPtr<MClient>& client, bool ok);

protected:
	void init_cmd_iov(int count);

protected:
	ostk_t *_ostk;		// NB: DON'T destroy _ostk in destruction function
	MCallbackPtr _callback;
	MOCategory _category;

	bool _zip;
	int _cmd_iov_count; 
	struct iovec *_cmd_iov;

	MValue *_mvals;
	int _mvals_use;;
	int _mvals_cap;

	uint64_t _start_tsc;
};

struct MO_version: public MOperation
{
	MO_version(const MCallbackPtr& cb);
};

struct MO_set: public MOperation
{
	MO_set(const MCallbackPtr& cb, const xstr_t& key, const xstr_t& value, int expire, uint32_t flag);
};

struct MO_replace: public MOperation
{
	MO_replace(const MCallbackPtr& cb, const xstr_t& key, const xstr_t& value, int expire, uint32_t flag);
};

struct MO_add: public MOperation
{
	MO_add(const MCallbackPtr& cb, const xstr_t& key, const xstr_t& value, int expire, uint32_t flag);
};

struct MO_cas: public MOperation
{
	MO_cas(const MCallbackPtr& cb, const xstr_t& key, const xstr_t& value, int64_t revision, int expire, uint32_t flag);
};

struct MO_append: public MOperation
{
	MO_append(const MCallbackPtr& cb, const xstr_t& key, const xstr_t& value);
};

struct MO_prepend: public MOperation
{
	MO_prepend(const MCallbackPtr& cb, const xstr_t& key, const xstr_t& value);
};

struct MO_remove: public MOperation
{
	MO_remove(const MCallbackPtr& cb, const xstr_t& key);
};

struct MO_increment: public MOperation
{
	MO_increment(const MCallbackPtr& cb, const xstr_t& key, int64_t value);
};

struct MO_decrement: public MOperation
{
	MO_decrement(const MCallbackPtr& cb, const xstr_t& key, int64_t value);
};

struct MO_get: public MOperation
{
	MO_get(const MCallbackPtr& cb, const xstr_t& key);
};

struct MO_getMulti: public MOperation
{
	MO_getMulti(const MCallbackPtr& cb, const std::vector<xstr_t>& keys);
};

#endif
