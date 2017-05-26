#include "MyMethodTab.h"
#include "xslib/jenkins.h"


MyMethodTab::NodeType::NodeType(const xstr_t& key)
{
	mark = false;
	xatomic64_set(&ncall, 0);
	hash = jenkins_hash(key.data, key.len, 0);
	nlen = key.len;
	memcpy(name, key.data, key.len);
	name[key.len] = 0;
}


MyMethodTab::MyMethodTab()
{
	size_t slot_num = 128;
	_ostk = ostk_create(0);
	_tab = OSTK_CALLOC(_ostk, NodeType*, slot_num);
	_mask = slot_num - 1;
	_total = 0;
	_logOn = false;
	_markAll = false;
}

MyMethodTab::~MyMethodTab()
{
	for (uint32_t slot = 0; slot <= _mask; ++slot)
	{
		NodeType *node, *next;
		for (node = _tab[slot]; node; node = next)
		{
			next = node->hash_next;
			node->NodeType::~NodeType();
		}
	}
	ostk_destroy(_ostk);
}

MyMethodTab::NodeType* MyMethodTab::insert(const xstr_t& name)
{
	NodeType *node;
	uint32_t hash = jenkins_hash(name.data, name.len, 0);
	uint32_t slot = (hash & _mask);

	for (node = _tab[slot]; node; node = node->hash_next)
	{
		if (node->hash == hash && node->nlen == name.len && memcmp(node->name, name.data, name.len) == 0)
			return NULL;
	}

	void *p = ostk_alloc(_ostk, sizeof(NodeType) + name.len + 1);
	node = new(p) NodeType(name);
	node->hash_next = _tab[slot];
	_tab[slot] = node;
	_total++;
	return node;
}

MyMethodTab::NodeType* MyMethodTab::find(const xstr_t& key) const
{
	NodeType *node;
	uint32_t hash = jenkins_hash(key.data, key.len, 0);
	uint32_t slot = (hash & _mask);
	for (node = _tab[slot]; node; node = node->hash_next)
	{
		if (node->hash == hash && node->nlen == key.len && memcmp(node->name, key.data, key.len) == 0)
			return node;
	}
	return NULL;
}

MyMethodTab::NodeType* MyMethodTab::next(const NodeType *node) const
{
	uint32_t slot = 0;
	if (node)
	{
		if (node->hash_next)
			return node->hash_next;
		else
		{
			slot = node->hash & _mask;
			++slot;
			if (slot > _mask)
				return NULL;
		}
	}

	while (slot <= _mask)
	{
		if (_tab[slot])
			return _tab[slot];
		++slot;
	}

	return NULL;
}

void MyMethodTab::mark(const xstr_t& method, bool on) const
{
	NodeType* node = find(method);
	if (node)
		node->mark = on;
}

void MyMethodTab::markMany(const xstr_t& methods, bool on) const
{
	xstr_t tmp = methods;
	xstr_t xs;
	while (xstr_token_cstr(&tmp, ", ", &xs))
	{
		mark(xs, on);
	}
}


