#ifndef CallerKind_h_
#define CallerKind_h_

#include "xslib/xstr.h"
#include "xslib/XLock.h"
#include "xslib/LruHashMap.h"
#include <stdio.h>
#include <string>

#define WRITER_STICKY_INTERVAL	5	// seconds

class CallerKindMd5
{
	union {
		unsigned char md5[16];
		uint32_t u32[4];
	} _d;

public:
	CallerKindMd5(const std::string& con, const xstr_t& caller, const xstr_t& kind);

	unsigned int hash() const
	{
		return _d.u32[2];
	}

        bool operator==(const CallerKindMd5& r) const
        {
                return (memcmp(_d.md5, r._d.md5, sizeof(_d.md5)) == 0);
        }

        bool operator<(const CallerKindMd5& r) const
        {
                return (memcmp(_d.md5, r._d.md5, sizeof(_d.md5)) < 0);
        }
};


class CallerKindMap: private XMutex
{
	typedef LruHashMap<CallerKindMd5, time_t> TheMap;
	TheMap _map;
public:
	CallerKindMap();
	void replace(time_t now, const CallerKindMd5& ck);
	bool find(time_t now, const CallerKindMd5& ck);
	void reap(time_t now);
};


#endif
