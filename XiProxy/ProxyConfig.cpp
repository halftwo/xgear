#include "ProxyConfig.h"
#include "xic/EngineImp.h"
#include "dlog/dlog.h"
#include "xslib/xstr.h"
#include "xslib/XError.h"
#include "xslib/Enforce.h"
#include "xslib/ScopeGuard.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sstream>


ProxyConfig::ProxyConfig(const std::string& listfile)
	: _listfile(listfile), _listfile_mtime(0), _last_revision(0)
{
	if (_listfile.empty())
		throw XERROR_MSG(XError, "listfile is not a valid filename");
	reload();
}

bool ProxyConfig::find(const std::string& id, ProxyDetail& res)
{
	Lock lock(*this);
	ProxyMap::iterator iter = _proxy_map.find(id);
	if (iter != _proxy_map.end())
	{
		res = iter->second;
		return true;
	}
	dlog("NOTICE", "Can't find Proxy in ProxyConfig, id=%s, file=%s", id.c_str(), _listfile.c_str());
	return false;
}

void ProxyConfig::_add_item(ProxyMap& proxy_map, const std::string& key, ProxyDetail& pd)
{
	if (key.empty())
		return;

	xstr_t tmp = XSTR_CXX(pd.value);
	xstr_trim(&tmp);
	if (tmp.len == 0)
		return;

	if (pd.type == ExternalProxy)
	{
		std::ostringstream os;
		xstr_t endpoint;
		while (xstr_delimit_char(&tmp, '@', &endpoint))
		{
			xstr_trim(&endpoint);
			if (endpoint.len == 0)
				continue;

			try {
				xic::EndpointInfo ei;
				xic::parseEndpoint(endpoint, ei);
				os << "@" << ei.proto << '+' << ei.host << '+' << ei.port;
				if (ei.timeout > 0 || ei.close_timeout > 0 || ei.connect_timeout > 0)
				{
					os << " timeout=" << ei.timeout
						<< ',' << ei.close_timeout
						<< ',' << ei.connect_timeout;
				}
			}
			catch (std::exception& ex)
			{
				dlog("ERROR", "Invalid endpoint (%.*s) for service (%s): %s",
					XSTR_P(&endpoint), key.c_str(), ex.what());
			}
		}
		os.flush();
		pd.value = os.str();
	}
	else if ((size_t)tmp.len < pd.value.length())
	{
		pd.value = make_string(tmp);
	}

	ProxyMap::iterator iter = _proxy_map.find(key);
	if (iter != _proxy_map.end() && iter->second.value == pd.value && iter->second.option == pd.option)
		pd.revision = iter->second.revision;
	else
		pd.revision = ++_last_revision;
	proxy_map[key] = pd;
}

bool ProxyConfig::reload()
{
	struct stat st;

	if (stat(_listfile.c_str(), &st) == -1)
	{
		throw XERROR_FMT(XError, "stat() failed, file=%s", _listfile.c_str());
	}

	if (st.st_mtime != _listfile_mtime)
	{
		_listfile_mtime = st.st_mtime;

		dlog("LOAD_LISTFILE", "load_proxies() from file %s", _listfile.c_str());
		FILE *fp = ENFORCE(fopen(_listfile.c_str(), "rb"));
		ON_BLOCK_EXIT(fclose, fp);

		ProxyMap proxies;
		std::string key;
		ProxyDetail pd;
		int len;
		char *buf = NULL;
		size_t buf_size = 0;
		bool item_started = false;
		int lineno = 0;
		while ((len = getline(&buf, &buf_size, fp)) > 0)
		{
			lineno++;
			xstr_t xs = XSTR_INIT((unsigned char *)buf, len);
			xstr_trim(&xs);

			if (xs.len == 0)
			{
				item_started = false;
				continue;
			}
			if (xs.data[0] == '#')
			{
				continue;
			}

			if (xs.data[0] == '@')
			{
				if (!item_started)
				{
					dlog("WARNING", "Unknown proxy identity at line %d in file %s", lineno, _listfile.c_str());
					continue;
				}

				pd.value += ' ' + make_string(xs);
			}
			else if (xs.data[0] == '=')
			{
				if (!item_started)
				{
					dlog("WARNING", "Unknown proxy identity at line %d in file %s", lineno, _listfile.c_str());
					continue;
				}

				xs.data[0] = ' ';
				pd.value += make_string(xs);
			}
			else if (xs.data[0] == '!')
			{
				_add_item(proxies, key, pd);

				xstr_t k, v;
				xstr_advance(&xs, 1);
				if (xstr_key_value(&xs, '=', &k, &v) < 0 || k.len == 0)
				{
					dlog("WARNING", "Internal proxy line must have '=' at line %d in file %s", lineno, _listfile.c_str());
					continue;
				}

				key = make_string(k);
				pd.value = make_string(v);
				pd.type = InternalProxy;
				item_started = true;
			}
			else
			{
				_add_item(proxies, key, pd);

				xstr_t k, v;
				if (xstr_key_value(&xs, '@', &k, &v) < 0 || k.len == 0)
				{
					dlog("WARNING", "External proxy line must have '@' (endpoint) at line %d in file %s", lineno, _listfile.c_str());
					continue;
				}

				xstr_t identity;
				xstr_token_space(&k, &identity);
				key = make_string(identity);
				pd.value = v.len ? "@" + make_string(v) : "";
				pd.option = make_string(k);
				pd.type = ExternalProxy;
				item_started = true;
			}
		}
		free(buf);

		_add_item(proxies, key, pd);

		Lock lock(*this);
		_proxy_map.swap(proxies);
		return true;
	}

	return false;
}

