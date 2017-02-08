#include "CallerKind.h"
#include "xslib/md5.h"

CallerKindMd5::CallerKindMd5(const std::string& con, const xstr_t& caller, const xstr_t& kind)
{
	md5_context ctx;
	md5_start(&ctx);
	md5_update(&ctx, con.data(), con.length());
	md5_update(&ctx, caller.data, caller.len);
	md5_update(&ctx, kind.data, kind.len);
	md5_finish(&ctx, _d.md5);
}

CallerKindMap::CallerKindMap()
	: _map(65536, INT_MAX)
{
}

void CallerKindMap::replace(time_t now, const CallerKindMd5& ck)
{
	Lock lock(*this);
	_map.replace(ck, now);
}

bool CallerKindMap::find(time_t now, const CallerKindMd5& ck)
{
	Lock lock(*this);
	TheMap::node_type *node = _map.find(ck);
	if (node)
	{
		if(node->data > now - WRITER_STICKY_INTERVAL)
			return true;

		_map.remove_node(node);
	}

	return false;
}

void CallerKindMap::reap(time_t now)
{
	time_t expire = now - WRITER_STICKY_INTERVAL;

	Lock lock(*this);
	TheMap::node_type *node, *next;
	for (node = _map.most_stale(); node; node = next)
	{
		if (node->data > expire)
			break;

		next = _map.next_stale(node);
		_map.remove_node(node);
	}
}

