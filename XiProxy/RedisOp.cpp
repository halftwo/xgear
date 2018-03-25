#include "RedisOp.h"
#include "RedisClient.h"
#include "xslib/rdtsc.h"
#include "xslib/ostk.h"
#include "xslib/vbs.h"
#include "xslib/cxxstr.h"
#include "xslib/xlog.h"
#include "dlog/dlog.h" 

#define SLOW_MSEC	400
#define CHUNK_SIZE	1024
#define DEFAULT_EXPIRE	(86400*7*7)	// 7 weeks


void *RedisOperation::operator new(size_t size)
{
	ostk_t *ostk = ostk_create(CHUNK_SIZE);
	void *p = ostk_hold(ostk, size);
	return p;
}

void RedisOperation::operator delete(void *p)
{
	if (p)
	{
		ostk_t *ostk = &((ostk_t *)p)[-1];
		ostk_destroy(ostk);
	}
}

RedisOperation::RedisOperation(const RedisResultCallbackPtr& callback)
	: _callback(callback)
{
	this->_ostk = &((ostk_t *)this)[-1];
	rope_init(&_rope, 200, &ostk_xmem, _ostk);
	_cmd_num = 0;
	_cmd_iov = NULL;
	_cmd_iov_count = 0;
	vbs_list_init(&_replies, 0);
	_start_tsc = rdtsc();
}

struct iovec *RedisOperation::get_iovec(int *count)
{
	if (!_cmd_iov)
	{
		_cmd_iov_count = _rope.block_count;
		_cmd_iov = OSTK_ALLOC(_ostk, struct iovec, _cmd_iov_count);
		rope_iovec(&_rope, _cmd_iov);
	}
	*count = _cmd_iov_count;
	return _cmd_iov;
}

void RedisOperation::one_reply(const vbs_data_t& d)
{
	vbs_litem_t *entry = OSTK_ALLOC_ONE(_ostk, vbs_litem_t);
	entry->value = d;
	vbs_list_push_back(&_replies, entry);
}

void RedisOperation::finish(RedisClient* client, const std::exception& ex)
{
	int msec = (rdtsc() - _start_tsc) * 1000 / cpu_frequency();
	if (msec > SLOW_MSEC)
	{
		xstr_t caller = _callback->caller();
		dlog("RDS_SLOW", "time=%d.%03d caller=%.*s service=%s server=%s cmd=%.*s",
			msec/1000, msec%1000, XSTR_P(&caller),
			client ? client->service().c_str() : "",
			client ? client->server().c_str() : "",
			(int)_cmd_iov[0].iov_len, (char *)_cmd_iov[0].iov_base);
	}

	_callback->exception(ex);
}

bool RedisOperation::finish(RedisClient* client)
{
	int msec = (rdtsc() - _start_tsc) * 1000 / cpu_frequency();
	if (msec > SLOW_MSEC)
	{
		xstr_t caller = _callback->caller();
		dlog("RDS_SLOW", "time=%d.%03d caller=%.*s service=%s server=%s cmd=%.*s",
			msec/1000, msec%1000, XSTR_P(&caller),
			client ? client->service().c_str() : "",
			client ? client->server().c_str() : "",
			(int)_cmd_iov[0].iov_len, (char *)_cmd_iov[0].iov_base);
	}

	return _callback->completed(_replies);
}

static ssize_t rope_printf(rope_t *rope, const char *fmt, ...) XS_C_PRINTF(2,3);

ssize_t rope_printf(rope_t *rope, const char *fmt, ...)
{
	char buf[256];
	va_list ap;
	ssize_t rc;
	
	va_start(ap, fmt);
	rc = vxformat(NULL, (xio_write_function)rope_write, rope, buf, sizeof(buf), fmt, ap);
	va_end(ap);
	return rc;
}

static void one_arg(rope_t *rope, const xstr_t& s)
{
	rope_printf(rope, "$%zu\r\n", s.len);
	rope_write(rope, s.data, s.len);
	rope_puts(rope, "\r\n");
}

static void one_arg(rope_t *rope, const char* s)
{
	int n = strlen(s);
	rope_printf(rope, "$%d\r\n", n);
	rope_write(rope, s, n);
	rope_puts(rope, "\r\n");
}

static void one_arg(rope_t *rope, intmax_t i)
{
	char buf[24];
	int n = snprintf(buf, sizeof(buf), "%jd", i);
	rope_printf(rope, "$%d\r\n", n);
	rope_write(rope, buf, n);
	rope_puts(rope, "\r\n");
}

static void one_arg(rope_t *rope, double r)
{
	char buf[24];
	int n = snprintf(buf, sizeof(buf), "%g", r);
	rope_printf(rope, "$%d\r\n", n);
	rope_write(rope, buf, n);
	rope_puts(rope, "\r\n");
}

static void one_arg(rope_t *rope, bool b)
{
	rope_puts(rope, "$1\r\n");
	rope_puts(rope, b ? "1\r\n" : "0\r\n");
}

static void one_arg(rope_t *rope, const vbs_data_t& d)
{
	switch (d.kind)
	{
	case VBS_STRING:
		one_arg(rope, d.d_xstr);
		break;
	case VBS_BLOB:
		one_arg(rope, d.d_blob);
		break;
	case VBS_INTEGER:
		one_arg(rope, d.d_int);
		break;
	case VBS_FLOATING:
		one_arg(rope, d.d_floating);
		break;
	case VBS_BOOL:
		one_arg(rope, d.d_bool);
		break;
	default:
		throw XERROR_MSG(XError, "invalid arg");
	}
}

static void one_cmd(rope_t* rope, const vbs_list_t* cmd)
{
	if (cmd->count < 1)
		throw XERROR_MSG(XError, "empty cmd");

	rope_printf(rope, "*%zu\r\n", cmd->count);

	vbs_litem_t *ent = cmd->first;
	if (ent->value.kind != VBS_STRING || ent->value.d_xstr.len == 0)
		throw XERROR_MSG(XError, "invalid cmd name");

	xstr_t& arg0 = ent->value.d_xstr;
	if (arg0.len >= 5)
	{
		char c0 = arg0.data[0];
		char c1 = arg0.data[1];
		/* 
		   MULTI  PSUBSCRIBE  PUBLISH  PUNSUBSCRIBE  SUBSCRIBE  UNSUBSCRIBE  
		 */
		#define EQ(XS, CS)	xstr_case_equal_cstr((XS), (CS))
		if (((c0 == 'M' || c0 == 'm') && (c1 == 'U' || c1 == 'u') && EQ(&arg0, "MULTI"))
			|| ((c0 == 'P' || c0 == 'p') && (EQ(&arg0, "PSUBSCRIBE") || EQ(&arg0, "PUBLISH") || EQ(&arg0, "PUNSUBSCRIBE")))
			|| ((c0 == 'S' || c0 == 's') && (c1 == 'U' || c1 == 'u') && EQ(&arg0, "SUBSCRIBE"))
			|| ((c0 == 'U' || c0 == 'u') && (c1 == 'N' || c1 == 'n') && EQ(&arg0, "UNSUBSCRIBE")))
		{
			throw XERROR_FMT(XError, "Not allowed cmd (%.*s)", XSTR_P(&arg0));
		}
		#undef EQ
	}

	one_arg(rope, arg0);
	for (ent = ent->next; ent; ent = ent->next)
	{
		one_arg(rope, ent->value);
	}
}

RO_1call::RO_1call(const RedisResultCallbackPtr& cb, const vbs_list_t* cmd)
	: RedisOperation(cb)
{
	one_cmd(&_rope, cmd);
	++_cmd_num;
}

RO_ncall::RO_ncall(const RedisResultCallbackPtr& cb, const vbs_list_t* cmds)
	: RedisOperation(cb)
{
	if (cmds->count < 1)
		throw XERROR_MSG(XError, "no cmds");

	for (vbs_litem_t *cmd_ent = cmds->first; cmd_ent; cmd_ent = cmd_ent->next)
	{
		if (cmd_ent->value.kind != VBS_LIST)
			throw XERROR_MSG(XError, "invalid cmd");

		vbs_list_t *cmd = cmd_ent->value.d_list;
		one_cmd(&_rope, cmd);
		++_cmd_num;
	}
}

RO_tcall::RO_tcall(const RedisResultCallbackPtr& cb, const vbs_list_t* cmds)
	: RedisOperation(cb)
{
	if (cmds->count < 1)
		throw XERROR_MSG(XError, "no cmds");

	rope_puts(&_rope, "*1\r\n");
	one_arg(&_rope, "MULTI");
	++_cmd_num;

	for (vbs_litem_t *cmd_ent = cmds->first; cmd_ent; cmd_ent = cmd_ent->next)
	{
		if (cmd_ent->value.kind != VBS_LIST)
			throw XERROR_MSG(XError, "invalid cmd");

		vbs_list_t *cmd = cmd_ent->value.d_list;
		one_cmd(&_rope, cmd);
		++_cmd_num;
	}

	rope_puts(&_rope, "*1\r\n");
	one_arg(&_rope, "EXEC");
	++_cmd_num;
}

RO_auth::RO_auth(const RedisResultCallbackPtr& cb, const xstr_t& password)
	: RedisOperation(cb)
{
	rope_puts(&_rope, "*2\r\n");
	one_arg(&_rope, "AUTH");
	one_arg(&_rope, password);
	++_cmd_num;
}

RO_set::RO_set(const RedisResultCallbackPtr& cb, const xstr_t& key, const xstr_t& value, int expire)
	: RedisOperation(cb)
{
	rope_puts(&_rope, "*3\r\n");
	one_arg(&_rope, "SET");
	one_arg(&_rope, key);
	one_arg(&_rope, value);
	++_cmd_num;

	if (expire >= 0)
	{
		if (expire == 0)
			expire = DEFAULT_EXPIRE;

		rope_puts(&_rope, "*3\r\n");
		one_arg(&_rope, "EXPIRE");
		one_arg(&_rope, key);
		one_arg(&_rope, (intmax_t)expire);
		++_cmd_num;
	}
}

RO_remove::RO_remove(const RedisResultCallbackPtr& cb, const xstr_t& key)
	: RedisOperation(cb)
{
	rope_puts(&_rope, "*2\r\n");
	one_arg(&_rope, "DEL");
	one_arg(&_rope, key);
	++_cmd_num;
}

RO_increment::RO_increment(const RedisResultCallbackPtr& cb, const xstr_t& key, int64_t value)
	: RedisOperation(cb)
{
	rope_puts(&_rope, "*3\r\n");
	one_arg(&_rope, "INCRBY");
	one_arg(&_rope, key);
	one_arg(&_rope, value);
	++_cmd_num;
}

RO_decrement::RO_decrement(const RedisResultCallbackPtr& cb, const xstr_t& key, int64_t value)
	: RedisOperation(cb)
{
	rope_puts(&_rope, "*3\r\n");
	one_arg(&_rope, "DECRBY");
	one_arg(&_rope, key);
	one_arg(&_rope, value);
	++_cmd_num;
}

RO_get::RO_get(const RedisResultCallbackPtr& cb, const xstr_t& key)
	: RedisOperation(cb)
{
	rope_puts(&_rope, "*2\r\n");
	one_arg(&_rope, "GET");
	one_arg(&_rope, key);
	++_cmd_num;
}

RO_mget::RO_mget(const RedisResultCallbackPtr& cb, const std::vector<xstr_t>& keys)
	: RedisOperation(cb)
{
	size_t size = keys.size();
	if (size == 0)
		throw XERROR_FMT(XLogicError, "no key given");

	rope_printf(&_rope, "*%zd\r\n", 1 + size);
	one_arg(&_rope, "MGET");
	for (size_t i = 0; i < size; ++i)
	{
		one_arg(&_rope, keys[i]);
	}
	++_cmd_num;
}

