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
		mutable xatomic64_t ncall;
		uint32_t hash;
		uint32_t nlen;
		bool mark;
		char name[];
	};

	MyMethodTab();
	~MyMethodTab();

	NodeType* insert(const xstr_t& name);
	NodeType* find(const xstr_t& name) const;
	NodeType* next(const NodeType *node) const;

	void mark(const xstr_t& method, bool on=true) const;
	void markMany(const xstr_t& methods, bool on=true) const;

	bool logOn() const		{ return _logOn; }
	void logOn(bool t)		{ _logOn = t; }

	bool markAll() const		{ return _markAll; }
	void markAll(bool t)		{ _markAll = t; }

private:
	ostk_t *_ostk;
	NodeType **_tab;
	unsigned int _mask;
	unsigned int _total;
	bool _logOn;
	bool _markAll;
};

#endif
