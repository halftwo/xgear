#ifndef MyMethodTab_h_
#define MyMethodTab_h_

#include "xslib/ostk.h"
#include "xslib/xatomic.h"
#include <stdint.h>

struct MyMethodTab
{
	class NodeType
	{
		friend struct MyMethodTab;
		NodeType(const xstr_t& name);
		NodeType* hash_next;
	public:
		mutable xatomiclong_t ncall;
		uint32_t hash;
		uint32_t nlen;
		bool mark;
		char name[];
	};

	MyMethodTab();
	~MyMethodTab();

	NodeType* getOrAdd(const xstr_t& name);
	NodeType* find(const xstr_t& name) const;
	NodeType* next(const NodeType *node) const;

	void remove_node(NodeType *node);

	/* return true if found, else return false */
	bool mark(const xstr_t& method, bool on);

	bool markAll() const		{ return _markAll; }
	void markAll(bool t)		{ _markAll = t; }

private:
	NodeType **_tab;
	unsigned int _mask;
	unsigned int _total;
	bool _markAll;
};

#endif
