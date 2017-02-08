#include "MOperation.h"
#include "lz4codec.h"
#include "MClient.h"
#include "xslib/rdtsc.h"
#include "xslib/ostk.h"
#include "xslib/vbs.h"
#include "xslib/cxxstr.h"
#include "dlog/dlog.h" 

#define SLOW_MSEC	400
#define CHUNK_SIZE	256


void *MOperation::operator new(size_t size)
{
	ostk_t *ostk = ostk_create(CHUNK_SIZE);
	void *p = ostk_hold(ostk, size);
	return p;
}

void MOperation::operator delete(void *p)
{
	if (p)
	{
		ostk_t *ostk = &((ostk_t *)p)[-1];
		ostk_destroy(ostk);
	}
}

MOperation::MOperation(const MCallbackPtr& callback)
	: _callback(callback)
{
	this->_ostk = &((ostk_t *)this)[-1];
	_zip = false;
	_cmd_iov = NULL;
	_cmd_iov_count = 0;
	_category = MOC_NONE;
	_mvals = NULL;
	_mvals_use = 0;
	_mvals_cap = 0;
	_start_tsc = rdtsc();
}

struct iovec *MOperation::get_iovec(int *count)
{
	*count = _cmd_iov_count;
	return _cmd_iov;
}

void MOperation::init_cmd_iov(int count)
{
	_cmd_iov_count = count;
	_cmd_iov = (struct iovec *)ostk_alloc(_ostk, sizeof(_cmd_iov[0]) * _cmd_iov_count);
}

bool MOperation::appendMValue(const MValue& mv)
{
	if (_mvals_use < _mvals_cap)
	{
		_mvals[_mvals_use++] = mv;
		return true;
	}

	return false;
}

void MOperation::informCallback()
{
	if (_mvals_use)
	{
		this->xref_inc();
		_callback->received(_mvals, _mvals_use, true, (void (*)(void *))decrement_xref_count, (XRefCount*)this);
		_mvals_use = 0;
	}
}

void MOperation::finish(const XPtr<MClient>& client, bool ok)
{
	int msec = (rdtsc() - _start_tsc) * 1000 / cpu_frequency();
	if (msec > SLOW_MSEC)
	{
		xstr_t caller = _callback->caller();
		dlog("MC_SLOW", "time=%d.%03d caller=%.*s service=%s server=%s cmd=%.*s",
			msec/1000, msec%1000, XSTR_P(&caller),
			client->service().c_str(), client->server().c_str(),
			(int)_cmd_iov[0].iov_len, (char *)_cmd_iov[0].iov_base);
	}
	_callback->completed(ok, _zip);
}

static inline void check_key(const xstr_t& key)
{
	if (key.len == 0 || key.len > 250 || xstr_find_in_bset(&key, 0, &space_bset) >= 0)
		throw XERROR_MSG(XError, xformat_string(vbs_xfmt, "key for memcache can't be empty or larger than 250 bytes or contain whitespace, key=%p{>VBS_STRING<}", &key));
}

static inline struct iovec x2o(const xstr_t& xs)
{
	struct iovec iov = { xs.data, (size_t)xs.len };
	return iov;
}

static struct iovec crnl = { (void *)"\r\n", 2 };

MO_version::MO_version(const MCallbackPtr& cb)
	: MOperation(cb)
{
	_category = MOC_VERSION;
	init_cmd_iov(1);
	_cmd_iov[0] = x2o(ostk_xstr_dup_cstr(_ostk, "version\r\n"));
}

static inline bool _attempt_zip(ostk_t *ostk, const xstr_t& key, const xstr_t& value, xstr_t& out, uint32_t& flags)
{
	out = value;
	if (flags & FLAG_LZ4_ZIP)
	{
		int rc = attempt_lz4_zip(ostk, value, out);
		if (rc < 0)
		{
			flags &= ~FLAG_LZ4_ZIP;
			dlog("MC_ZIP", "attempt_lz4_zip()=%d key=%.*s, value.len=%zd", rc, XSTR_P(&key), value.len);
		}
	}
	return (flags & FLAG_LZ4_ZIP);
}

/* <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n */

MO_set::MO_set(const MCallbackPtr& cb, const xstr_t& key, const xstr_t& value, int expire, uint32_t flags)
	: MOperation(cb)
{
	_category = MOC_STORE;
	check_key(key);
	xstr_t v;
	_zip = _attempt_zip(_ostk, key, value, v, flags);
	init_cmd_iov(3);
	_cmd_iov[0] = x2o(ostk_xstr_printf(_ostk, "set %.*s %u %d %zd\r\n", XSTR_P(&key), flags, expire, v.len));
	_cmd_iov[1] = x2o(v);	// the callback or myself owns the value
	_cmd_iov[2] = crnl;
}

MO_replace::MO_replace(const MCallbackPtr& cb, const xstr_t& key, const xstr_t& value, int expire, uint32_t flags)
	: MOperation(cb)
{
	_category = MOC_STORE;
	check_key(key);
	xstr_t v;
	_zip = _attempt_zip(_ostk, key, value, v, flags);
	init_cmd_iov(3);
	_cmd_iov[0] = x2o(ostk_xstr_printf(_ostk, "replace %.*s %u %d %zd\r\n", XSTR_P(&key), flags, expire, v.len));
	_cmd_iov[1] = x2o(v);	// the callback or myslef owns the value
	_cmd_iov[2] = crnl;
}

MO_add::MO_add(const MCallbackPtr& cb, const xstr_t& key, const xstr_t& value, int expire, uint32_t flags)
	: MOperation(cb)
{
	_category = MOC_STORE;
	check_key(key);
	xstr_t v;
	_zip = _attempt_zip(_ostk, key, value, v, flags);
	init_cmd_iov(3);
	_cmd_iov[0] = x2o(ostk_xstr_printf(_ostk, "add %.*s %u %d %zd\r\n", XSTR_P(&key), flags, expire, v.len));
	_cmd_iov[1] = x2o(v);	// the callback or myself owns the value
	_cmd_iov[2] = crnl;
}

MO_cas::MO_cas(const MCallbackPtr& cb, const xstr_t& key, const xstr_t& value, int64_t revision, int expire, uint32_t flags)
	: MOperation(cb)
{
	_category = MOC_CAS;
	check_key(key);
	xstr_t v;
	_zip = _attempt_zip(_ostk, key, value, v, flags);
	init_cmd_iov(3);
	_cmd_iov[0] = x2o(ostk_xstr_printf(_ostk, "cas %.*s %u %d %zd %jd\r\n", XSTR_P(&key), flags, expire, v.len, (intmax_t)revision));
	_cmd_iov[1] = x2o(v);	// the callback or myself owns the value
	_cmd_iov[2] = crnl;
}

MO_append::MO_append(const MCallbackPtr& cb, const xstr_t& key, const xstr_t& value)
	: MOperation(cb)
{
	_category = MOC_STORE;
	check_key(key);
	init_cmd_iov(3);
	_cmd_iov[0] = x2o(ostk_xstr_printf(_ostk, "append %.*s 0 0 %zd\r\n", XSTR_P(&key), value.len));
	_cmd_iov[1] = x2o(value);	// the callback owns the value
	_cmd_iov[2] = crnl;
}

MO_prepend::MO_prepend(const MCallbackPtr& cb, const xstr_t& key, const xstr_t& value)
	: MOperation(cb)
{
	_category = MOC_STORE;
	check_key(key);
	init_cmd_iov(3);
	_cmd_iov[0] = x2o(ostk_xstr_printf(_ostk, "prepend %.*s 0 0 %zd\r\n", XSTR_P(&key), value.len));
	_cmd_iov[1] = x2o(value);	// the callback owns the value
	_cmd_iov[2] = crnl;
}

MO_remove::MO_remove(const MCallbackPtr& cb, const xstr_t& key)
	: MOperation(cb)
{
	_category = MOC_DELETE;
	check_key(key);
	init_cmd_iov(1);
	_cmd_iov[0] = x2o(ostk_xstr_printf(_ostk, "delete %.*s\r\n", XSTR_P(&key)));
}

MO_increment::MO_increment(const MCallbackPtr& cb, const xstr_t& key, int64_t value)
	: MOperation(cb)
{
	_category = MOC_COUNT;
	check_key(key);
	init_cmd_iov(1);
	_cmd_iov[0] = x2o(ostk_xstr_printf(_ostk, "incr %.*s %jd\r\n", XSTR_P(&key), (intmax_t)value));
}

MO_decrement::MO_decrement(const MCallbackPtr& cb, const xstr_t& key, int64_t value)
	: MOperation(cb)
{
	_category = MOC_COUNT;
	check_key(key);
	init_cmd_iov(1);
	_cmd_iov[0] = x2o(ostk_xstr_printf(_ostk, "decr %.*s %jd\r\n", XSTR_P(&key), (intmax_t)value));
}

MO_get::MO_get(const MCallbackPtr& cb, const xstr_t& key)
	: MOperation(cb)
{
	_category = MOC_GET;
	check_key(key);
	init_cmd_iov(1);
	_cmd_iov[0] = x2o(ostk_xstr_printf(_ostk, "gets %.*s\r\n", XSTR_P(&key)));
	_mvals_cap = 1;
	_mvals = (MValue *)ostk_alloc(_ostk, sizeof(MValue) * _mvals_cap);
}

MO_getMulti::MO_getMulti(const MCallbackPtr& cb, const std::vector<xstr_t>& keys)
	: MOperation(cb)
{
	_category = MOC_GETMULTI;
	size_t size = keys.size();
	if (size == 0)
		throw XERROR_FMT(XLogicError, "no key given");

	init_cmd_iov(1);
	ostk_object_puts(_ostk, "gets");
	for (size_t i = 0; i < size; ++i)
	{
		const xstr_t& key = keys[i];
		check_key(key);
		ostk_object_putc(_ostk, ' ');
		ostk_object_grow(_ostk, key.data, key.len);
	}
	ostk_object_puts(_ostk, "\r\n");
	_cmd_iov[0].iov_base = ostk_object_finish(_ostk, &_cmd_iov[0].iov_len);
	_mvals_cap = size;
	_mvals = (MValue *)ostk_alloc(_ostk, sizeof(MValue) * _mvals_cap);
}

