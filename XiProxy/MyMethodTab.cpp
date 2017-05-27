#include "MyMethodTab.h"
#include "xslib/xsdef.h"
#include "xslib/jenkins.h"
#include <assert.h>


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
	_tab = XS_CALLOC(NodeType*, slot_num);
	_mask = slot_num - 1;
	_total = 0;
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
			free(node);
		}
	}
	free(_tab);
}

MyMethodTab::NodeType* MyMethodTab::getOrAdd(const xstr_t& key)
{
	NodeType *node;
	uint32_t hash = jenkins_hash(key.data, key.len, 0);
	uint32_t slot = (hash & _mask);
	for (node = _tab[slot]; node; node = node->hash_next)
	{
		if (node->hash == hash && node->nlen == key.len && memcmp(node->name, key.data, key.len) == 0)
			return node;
	}

	void *p = malloc(sizeof(NodeType) + key.len + 1);
	node = new(p) NodeType(key);
	node->hash_next = _tab[slot];
	_tab[slot] = node;
	++_total;
	return node;
}

void MyMethodTab::remove_node(NodeType *the_node)
{
	uint32_t slot = (the_node->hash & _mask);
	NodeType *node, *prev = NULL;
	for (node = _tab[slot]; node; prev = node, node = node->hash_next)
	{
		if (node == the_node)
		{
			if (prev)
				prev->hash_next = node->hash_next;
			else
				_tab[slot] = node->hash_next;
			node->NodeType::~NodeType();
			free(node);
			--_total;
			return;
		}
	}
	assert(!"can't reach here");
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

bool MyMethodTab::mark(const xstr_t& method, bool on)
{
	NodeType* node = find(method);
	if (node)
	{
		node->mark = on;
		return true;
	}
	return false;
}


