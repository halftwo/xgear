#ifndef RCache_h_
#define RCache_h_

#include "xslib/xsdef.h"
#include "xslib/LruHashMap.h"
#include "xslib/vbs.h"
#include "xslib/xstr.h"
#include "xslib/oref.h"
#include "xslib/sha1.h"
#include "xslib/XLock.h"
#include "xslib/XRefCount.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>


enum RDataType
{
	RD_NONE,
	RD_ANSWER,
	RD_MCACHE,
	RD_LCACHE,
};


class RKey
{
	union {
		unsigned char sha1[20];
		uint32_t u32[5];
	} _d;

public:
	RKey()
	{
		memset(this->_d.sha1, 0, sizeof(this->_d.sha1));
	}

	RKey(RDataType type, const char *data, size_t len)
	{
		sha1_context ctx;
		sha1_start(&ctx);
		sha1_update(&ctx, &type, sizeof(type));
		sha1_update(&ctx, data, len);
		sha1_finish(&ctx, _d.sha1);
	}

	RKey(const xstr_t& lcache_key)
	{
		RDataType type = RD_LCACHE;
		sha1_context ctx;
		sha1_start(&ctx);
		sha1_update(&ctx, &type, sizeof(type));
		sha1_update(&ctx, lcache_key.data, lcache_key.len);
		sha1_finish(&ctx, _d.sha1);
	}

	RKey(const xstr_t& mcache, const xstr_t& key)
	{
		RDataType type = RD_MCACHE;
		sha1_context ctx;
		sha1_start(&ctx);
		sha1_update(&ctx, &type, sizeof(type));
		sha1_update(&ctx, key.data, key.len);
		sha1_finish(&ctx, _d.sha1);
	}

	RKey(const xstr_t& service, const xstr_t& method, const xstr_t& params)
	{
		set(service, method, params);
	}

	void set(const xstr_t& service, const xstr_t& method, const xstr_t& params)
	{
		RDataType type = RD_ANSWER;
		sha1_context ctx;
		sha1_start(&ctx);
		sha1_update(&ctx, &type, sizeof(type));
		sha1_update(&ctx, service.data, service.len);
		sha1_update(&ctx, method.data, method.len);
		sha1_update(&ctx, params.data, params.len);
		sha1_finish(&ctx, _d.sha1);
	}

	unsigned int hash() const
	{
		return _d.u32[2];
	}

	bool operator==(const RKey& r) const
	{
		return (memcmp(_d.sha1, r._d.sha1, sizeof(_d.sha1)) == 0);
	}

	bool operator<(const RKey& r) const
	{
		return (memcmp(_d.sha1, r._d.sha1, sizeof(_d.sha1)) < 0);
	}
};


class RData
{
	friend class RCache;
	struct rdata_t {
		OREF_DECLARE();
		int revision;
		uint64_t ctime;
		RDataType type;
		int32_t status;
		uint32_t length;
		unsigned char data[];
	};
	mutable rdata_t *_dat;

	static void *operator new(size_t size);

public:
	static void *ref_rdata(const RData& r)
	{
		if (r._dat)
		{
			OREF_INC(r._dat);
		}
		return r._dat;
	}

	static void unref_rdata(void *data)
	{
		if (data)
		{
			rdata_t *d = (rdata_t*)data;
			OREF_DEC(d, ::free);
		}
	}

public:
	RData(): _dat(0) {}

	RData(uint64_t ctime, RDataType type, const xstr_t& xs);

	RData(uint64_t ctime, RDataType type, const vbs_data_t& dat);

	RData(const RData& r): _dat(r._dat) 	{ if (_dat) OREF_INC(_dat); }

	RData& operator=(const RData& r)
	{
		if (_dat != r._dat)
		{
			if (r._dat) OREF_INC(r._dat);
			if (_dat) OREF_DEC(_dat, free);
			_dat = r._dat;
		}
		return *this;
	}

	~RData() 				{ if (_dat) OREF_DEC(_dat, free); }

	typedef rdata_t* RData::*my_pointer_bool;
        operator my_pointer_bool() const        { return _dat ? &RData::_dat : 0; }

	void setRevision(int revision) const	{ if (_dat) _dat->revision = revision; }
	void setStatus(uint32_t status) 	{ if (_dat) _dat->status = status; }

	int revision() const 			{ return _dat ? _dat->revision : 0; }
	uint64_t ctime() const 			{ return _dat ? _dat->ctime : 0; }
	RDataType type() const			{ return _dat ? _dat->type : RD_NONE; }
	int status() const			{ return _dat ? _dat->status : 0; }
	unsigned char *data() const 		{ return _dat ? _dat->data : NULL; }
	size_t length() const 			{ return _dat ? _dat->length : 0; }

	xstr_t xstr() const
	{
		xstr_t xs = xstr_null;
		if (_dat)
			xstr_init(&xs, _dat->data, _dat->length);
		return xs;
	}
};

class RCache: public XRefCount, private XMutex
{
public:
	RCache(size_t maxsize): _hashmap(maxsize), _revision(1) {}

	RData find(const RKey& key)
	{
		Lock lock(*this);
		HashMap::node_type* node = _hashmap.find(key);
		if (node && node->data.revision() == _revision)
			return node->data;
		return RData();
	}

	RData use(const RKey& key)
	{
		Lock lock(*this);
		HashMap::node_type* node = _hashmap.use(key);
		if (node && node->data.revision() == _revision)
			return node->data;
		return RData();
	}

	bool replace(const RKey& key, const RData& val)
	{
		val.setRevision(_revision);
		Lock lock(*this);
		return _hashmap.replace(key, val);
	}

	bool remove(const RKey& key)
	{
		Lock lock(*this);
		return _hashmap.remove(key);
	}

	intmax_t plus(const RKey& key, intmax_t val, uint64_t now, uint64_t after);

	size_t drain(size_t num)
	{
		Lock lock(*this);
		return _hashmap.drain(num);
	}

	size_t reap(size_t num, uint64_t before)
	{
		size_t n = 0;
		Lock lock(*this);
		for (n = 0; n < num; ++n)
		{
			HashMap::node_type* node = _hashmap.most_stale();
			if (node && (uint64_t)(before - node->data.ctime()) < INT64_MAX)
				_hashmap.remove_node(node);
			else
				break;
		}
		return n;
	}

	void clear()
	{
		++_revision;
	}

private:
	typedef LruHashMap<RKey, RData> HashMap;
	HashMap _hashmap;
	int _revision;
};

typedef XPtr<RCache> RCachePtr;

#endif
