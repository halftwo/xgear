#include "RCache.h"
#include "xslib/xstr.h"
#include "xslib/xbuf.h"
#include "xslib/rdtsc.h"
#include "xslib/vbs.h"

#define MIN_LENGTH	16	// must be greater than vbs_integer_size(INTMAX_MAX)


RData::RData(uint64_t ctime, RDataType type, const xstr_t& xs)
{
	uint32_t size = sizeof(rdata_t) + (xs.len > MIN_LENGTH ? xs.len : MIN_LENGTH);
	_dat = (rdata_t *)malloc(size);
	if (_dat)
	{
		OREF_INIT(_dat);
		_dat->revision = 0;
		_dat->ctime = ctime;
		_dat->type = type;
		_dat->status = 0;
		_dat->length = xs.len;
		memcpy(_dat->data, xs.data, xs.len);
	}
}

RData::RData(uint64_t ctime, RDataType type, const vbs_data_t& dat)
{
	size_t len = vbs_size_of_data(&dat);
	uint32_t size = sizeof(rdata_t) + (len > MIN_LENGTH ? len : MIN_LENGTH);
	_dat = (rdata_t *)malloc(size);
	if (_dat)
	{
		OREF_INIT(_dat);
		_dat->revision = 0;
		_dat->ctime = ctime;
		_dat->type = type;
		_dat->status = 0;
		_dat->length = len;
		xbuf_t xb = XBUF_INIT(_dat->data, _dat->length);
		vbs_packer_t pk = VBS_PACKER_INIT(xbuf_xio.write, &xb, -1);
		if (vbs_pack_data(&pk, &dat) != 0 || xb.len != xb.capacity)
		{
			free(_dat);
			throw XERROR(XError);
		}
	}
}

intmax_t RCache::plus(const RKey& key, intmax_t val, uint64_t now, uint64_t after)
{
	Lock lock(*this);
	HashMap::node_type* node = _hashmap.use(key);
	if (node && node->data.revision() == _revision && node->data.ctime() > after && node->data.type() == RD_LCACHE)
	{
		intmax_t oldval;
		vbs_unpacker_t uk = VBS_UNPACKER_INIT(node->data.data(), (ssize_t)node->data.length(), -1);
		if (vbs_unpack_integer(&uk, &oldval) == 0)
		{
			val += oldval;
			if (val != oldval)
			{
				node->data._dat->ctime = now;
				node->data._dat->length = vbs_buffer_of_integer(node->data._dat->data, val);
			}
			return val;
		}
	}

	unsigned char buf[MIN_LENGTH];
	int len = vbs_buffer_of_integer(buf, val);
	xstr_t xs = XSTR_INIT(buf, len);
	RData dat(now, RD_LCACHE, xs);
	dat.setRevision(_revision);
	_hashmap.replace(key, dat);
	return val;
}

