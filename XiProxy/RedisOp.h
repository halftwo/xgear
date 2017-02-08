#ifndef RedisOp_h_
#define RedisOp_h_

#include "xslib/XRefCount.h"
#include "xslib/ostk.h"
#include "xslib/rope.h"
#include "xslib/vbs_pack.h"
#include <sys/uio.h>
#include <vector>


class RedisClient;
class RedisResultCallback;
class RedisOperation;
typedef XPtr<RedisResultCallback> RedisResultCallbackPtr;
typedef XPtr<RedisOperation> RedisOperationPtr;


class RedisResultCallback: public XRefCount
{
public:
	virtual xstr_t caller() const 					= 0;
	virtual bool completed(const vbs_list_t& replies)		= 0;
	virtual void exception(const std::exception& ex)		= 0;
};

class RedisOperation: public XRefCount
{
public:
	static void *operator new(size_t size);
	static void operator delete(void *p);

public:
	RedisOperation(const RedisResultCallbackPtr& calback);

	ostk_t* ostk() const			{ return _ostk; }

	size_t cmd_num() const			{ return _cmd_num; }

	struct iovec *get_iovec(int *count);

	void one_reply(const vbs_data_t& d);

	bool finish(RedisClient* client);
	void finish(RedisClient* client, const std::exception& ex);

protected:
	ostk_t *_ostk;		// NB: DON'T destroy _ostk in destruction function
	RedisResultCallbackPtr _callback;
	rope_t _rope;
	int _cmd_num;
	int _cmd_iov_count; 
	struct iovec *_cmd_iov;
	vbs_list_t _replies;

	uint64_t _start_tsc;
};

struct RO_1call: public RedisOperation
{
	RO_1call(const RedisResultCallbackPtr& cb, const vbs_list_t* cmd);
};

struct RO_ncall: public RedisOperation
{
	RO_ncall(const RedisResultCallbackPtr& cb, const vbs_list_t* cmds);
};

struct RO_tcall: public RedisOperation
{
	RO_tcall(const RedisResultCallbackPtr& cb, const vbs_list_t* cmds);
};

struct RO_auth: public RedisOperation
{
	RO_auth(const RedisResultCallbackPtr& cb, const xstr_t& password);
};

struct RO_set: public RedisOperation
{
	RO_set(const RedisResultCallbackPtr& cb, const xstr_t& key, const xstr_t& value, int expire);
};

struct RO_remove: public RedisOperation
{
	RO_remove(const RedisResultCallbackPtr& cb, const xstr_t& key);
};

struct RO_increment: public RedisOperation
{
	RO_increment(const RedisResultCallbackPtr& cb, const xstr_t& key, int64_t value);
};

struct RO_decrement: public RedisOperation
{
	RO_decrement(const RedisResultCallbackPtr& cb, const xstr_t& key, int64_t value);
};

struct RO_get: public RedisOperation
{
	RO_get(const RedisResultCallbackPtr& cb, const xstr_t& key);
};

struct RO_mget: public RedisOperation
{
	RO_mget(const RedisResultCallbackPtr& cb, const std::vector<xstr_t>& keys);
};

#endif
