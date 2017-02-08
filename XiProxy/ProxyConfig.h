#ifndef ProxyConfig_h_
#define ProxyConfig_h_

#include "xslib/XLock.h"
#include <string>
#include <map>

enum ProxyType
{
	ExternalProxy,
	InternalProxy,
};


struct ProxyDetail
{
	ProxyType type;
	int revision;
	std::string option;
	std::string value;
public:
	ProxyDetail() : type(ExternalProxy), revision(0)
	{
	}
};

class ProxyConfig: private XMutex
{
public:
	ProxyConfig(const std::string& listfile);
	bool reload();
	bool find(const std::string& identity, ProxyDetail& res);

private:
	typedef std::map<std::string, ProxyDetail> ProxyMap;

	void _add_item(ProxyMap& proxy_map, const std::string& key, ProxyDetail& pd);

private:
	std::string _listfile;
	time_t _listfile_mtime;
	int _last_revision;
	ProxyMap _proxy_map;
};

#endif
