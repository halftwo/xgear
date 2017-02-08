#include "Policy.h"
#include "Response.h"
#include "xslib/xsdef.h"
#include "xslib/unixfs.h"
#include "xslib/ScopeGuard.h"
#include "xslib/vbs_json.h"
#include "xslib/ostk.h"
#include "xslib/cstr.h"
#include "xslib/xbuf.h"
#include "xic/VData.h"
#include "dlog/dlog.h"
#include "xslib/httpmisc.h"
#include "xslib/xbase64.h"
#include <sstream>


static size_t remove_comments(unsigned char *buf, size_t size)
{
	size_t n = 0;
	xstr_t xs = XSTR_INIT((unsigned char *)buf, size);
	xstr_t line;
	while (xstr_delimit_char(&xs, '\n', &line))
	{
		xstr_t s = line;
		xstr_ltrim(&s);
		if (s.len >= 2 && s.data[0] == '/' && s.data[1] == '/')
		{
			memmove(line.data, xs.data, xs.len);
		}
		else
		{
			if (n)
				++n;
			n += line.len;
		}
	}

	return n;
}

PolicyManager::PolicyManager(const xic::EnginePtr& engine, const std::string& file)
{
	unsigned char *buf = NULL;
	size_t size = 0;
	ssize_t len = unixfs_get_content(file.c_str(), &buf, &size);
	if (len < 0)
		throw XERROR_FMT(XError, "Failed to read file(%s)", file.c_str());
	ON_BLOCK_EXIT(free_pptr<unsigned char>, &buf);

	len = remove_comments(buf, len);
	if (len == 0)
		throw XERROR_FMT(XError, "Empty file(%s)", file.c_str());

	ostk_t *ostk = ostk_create(0);
	ON_BLOCK_EXIT(ostk_destroy, ostk);
	vbs_dict_t dict;
	if (json_unpack_vbs_dict(buf, len, &dict, &ostk_xmem, ostk) <= 0)
		throw XERROR_FMT(XError, "Failed to parse file(%s)", file.c_str());

	xic::ContextPtr ctx = xic::ContextBuilder("CALLER", "http2xic").build();

	xic::VDict d(&dict);
	xic::VDict locs = d.wantVDict("locations");
	for (xic::VDict::Node node = locs.first(); node; ++node)
	{
		std::string location = make_string(node.xstrKey());
		xic::VDict detail = node.vdictValue();

		xstr_t mode = detail.wantXstr("mode");
		PolicyPtr policy;
		if (xstr_equal_cstr(&mode, "bypass"))
		{
			BypassPolicyPtr p(new BypassPolicy);
			std::string service = make_string(detail.wantXstr("service"));
			p->prx = engine->stringToProxy(service);
			p->prx->setContext(ctx);
			p->method = make_string(detail.wantXstr("method"));
			p->oneway = detail.getBool("oneway");
			xstr_t type = detail.getXstr("args_type");
			if (type.len == 0 || xstr_equal_cstr(&type, "get_b64"))
			{
				p->argsType = BypassPolicy::ARGS_BASE64;
			}
			else if (xstr_equal_cstr(&type, "get_query"))
			{
				p->argsType = BypassPolicy::ARGS_QUERY;
			}
			else
			{
				throw XERROR_FMT(XError, "Invalid args_type (%.*s)", XSTR_P(&type));
			}
			policy = p;
		}
		else
		{
			throw XERROR_FMT(XError, "Unknown mode(%.*s)", XSTR_P(&mode));
		}

		policy->location = location;
		_policyMap.insert(std::make_pair(location, policy));
	}
}

PolicyManager::~PolicyManager()
{
}

PolicyPtr PolicyManager::getPolicy(const std::string& url)
{
	PolicyPtr policy;
	for (PolicyMap::iterator iter = _policyMap.begin(); iter != _policyMap.end(); ++iter)
	{
		const std::string& loc = iter->first;
		if (url.compare(0, loc.length(), loc) == 0)
		{
			policy = iter->second;
			break;
		}
	}

	return policy;
}

int BypassPolicy::_request(struct MHD_Connection *connection, const char *url, const xic::QuestPtr& q)
{
	if (this->oneway)
	{
		this->prx->emitQuest(q, xic::CompletionPtr());
		return respond_ok(connection);
	}
	else
	{
		xic::AnswerPtr a;
		try {
			a = this->prx->request(q);
		}
		catch (XError& ex)
		{
			dlog("EXCEPTION", "%s", ex.what());
			if (dynamic_cast<xic::QuestFailedException*>(&ex))
				return respond_bad_request(connection);
			else
				return respond_internal_server_error(connection);
		}

		xstr_t args = a->args_xstr();
		std::stringstream ss;
		const char *cb = url + location.length();
		if (cb[0])
			ss << cb << '(';
		vbs_to_json(args.data, args.len, ostream_xio.write, (std::ostream*)&ss, 0);
		if (cb[0])
			ss << ')';

		std::string out = ss.str();
		MHD_Response *response = MHD_create_response_from_buffer(out.length(), (void *)out.data(), MHD_RESPMEM_MUST_COPY);
		add_http2xic_header(response);
		MHD_add_response_header(response, MHD_HTTP_HEADER_CONTENT_TYPE, "application/json");
		int ret = MHD_queue_response(connection, MHD_HTTP_OK, response);
		MHD_destroy_response(response);
		return ret;
	}
}

static int querystring_iterator(void *cls, enum MHD_ValueKind kind, const char *key, const char *value)
{
	xic::VDictWriter *dw = (xic::VDictWriter*)cls;
	dw->kv(key, value);
	return MHD_YES;
}

int BypassPolicy::process(struct MHD_Connection *connection, const char *url, 
			const char *method, const char *version,
			const char *upload, size_t *upload_size, void **ptr)
{
	if (strcmp(method, MHD_HTTP_METHOD_POST) == 0)
	{
		if (*ptr == NULL)
		{
			xbuf_t *xb = XS_CALLOC_ONE(xbuf_t);
			if (!xb)
			{
				return respond_internal_server_error(connection);
			}

			*ptr = xb;
			size_t size = *upload_size;
			if (size < 4096)
				size = 4096;
			xb->data = (unsigned char *)malloc(size);
			if (!xb->data)
			{
				return respond_internal_server_error(connection);
			}
			memcpy(xb->data, upload, *upload_size);
			xb->capacity = size;
			xb->len = *upload_size;
			*upload_size = 0;
		}
		else if (*upload_size)
		{
			xbuf_t *xb = (xbuf_t *)*ptr;
			ssize_t size = xb->len + *upload_size;
			if (size > xb->capacity)
			{
				unsigned char *data = (unsigned char *)realloc(xb->data, size);
				if (!data)
					return respond_internal_server_error(connection);

				xb->data = data;
				xb->capacity = size;
			}

			memcpy(xb->data + xb->len, upload, *upload_size);
			xb->len = size;
			*upload_size = 0;
		}
		else if (((xbuf_t*)*ptr)->len)
		{
			xbuf_t *xb = (xbuf_t *)*ptr;
			xic::QuestPtr q = xic::Quest::create();
			q->setMethod(this->method.c_str());
            		q->setTxid(this->oneway ? 0 : -1);
			int rc = json_to_vbs(xb->data, xb->len, rope_xio.write, q->args_rope(), 0);
			if (rc < 0)
				throw XERROR_MSG(XArgumentError, "Request argument is not a valid json string");

			return _request(connection, url, q);
		}
	}
	else
	{
		xic::QuestPtr q;
		if (argsType == ARGS_QUERY)
		{
			xic::QuestWriter qw(this->method.c_str(), !this->oneway);
			xic::VDictWriter dw = qw.paramVDict("environ");
			MHD_get_connection_values(connection, MHD_GET_ARGUMENT_KIND, querystring_iterator, &dw);
			q = qw.take();
		}
		else // ARGS_BASE64
		{
			const char *value = MHD_lookup_connection_value(connection, MHD_GET_ARGUMENT_KIND, NULL);
			if (value && value[0])
			{
				size_t vlen = strlen(value);
				unsigned char *buf = (unsigned char *)malloc((vlen + 3) / 4 * 3);
				ssize_t n = xbase64_decode(&url_xbase64, buf, value, vlen, XBASE64_IGNORE_SPACE);
				if (n < 0)
					throw XERROR_MSG(XArgumentError, "Request query string is not a valid base64 encoded string");
				q = xic::Quest::create();
				q->setMethod(this->method.c_str());
				q->setTxid(this->oneway ? 0 : -1);
				int rc = json_to_vbs(buf, n, rope_xio.write, q->args_rope(), 0);
				if (rc < 0)
					throw XERROR_MSG(XArgumentError, "Request argument is not a valid json string");
			}
			else
			{
				xic::QuestWriter qw(this->method.c_str(), !this->oneway);
				q = qw.take();
			}
		}

		return _request(connection, url, q);
	}

	return MHD_YES;
}

void BypassPolicy::complete(struct MHD_Connection *connection, void **con_cls,
		enum MHD_RequestTerminationCode toe)
{
	xbuf_t *xb = (xbuf_t *)*con_cls;
	if (xb)
	{
		free(xb->data);
		free(xb);
	}
}

std::string ContentPolicy::getName(const char *url)
{
	std::string name = url;
	if (stripPrefix)
	{
		size_t pos = location.length();
		if (location[pos-1] == '/')
			--pos;
		name = name.substr(pos);
	}
	return name;
}

bool ContentPolicy::allowed(const std::string& name)
{
	bool allowed = false;
	for (std::set<std::string>::iterator iter = allowedPaths.begin(); iter != allowedPaths.end(); ++iter)
	{
		const std::string& prefix = *iter;
		if (name.compare(0, prefix.length(), prefix) == 0)
		{
			allowed = true;
			break;
		}
	}
	return allowed;
}



