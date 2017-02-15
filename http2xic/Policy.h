#ifndef Policy_h_
#define Policy_h_

#include "xic/Engine.h"
#include "xslib/XRefCount.h"
#include <microhttpd.h>
#include <string>
#include <set>
#include <map>


class PolicyManager;
struct Policy;
struct BypassPolicy;
struct StoragePolicy;
struct ThumbnailPolicy;
typedef XPtr<PolicyManager> PolicyManagerPtr;
typedef XPtr<Policy> PolicyPtr;
typedef XPtr<BypassPolicy> BypassPolicyPtr;
typedef XPtr<StoragePolicy> StoragePolicyPtr;
typedef XPtr<ThumbnailPolicy> ThumbnailPolicyPtr;

enum PolicyType
{
	POLICY_BYPASS,
	POLICY_STORAGE,
	POLICY_THUMBNAIL,
};

struct Policy: virtual public XRefCount
{
	std::string location;

	virtual int process(struct MHD_Connection *connection, const char *url, 
			const char *method, const char *version,
			const char *data, size_t *data_size, void **ptr)	= 0;

	virtual void complete(struct MHD_Connection *connection, void **con_cls,
			enum MHD_RequestTerminationCode toe)			= 0;
};


struct BypassPolicy: public Policy
{
	enum ArgsType
	{
		ARGS_BASE64,
		ARGS_QUERY,
	};

	xic::ProxyPtr prx;
	std::string method;
	bool oneway;
	ArgsType argsType;

	virtual int process(struct MHD_Connection *connection, const char *url, 
			const char *method, const char *version,
			const char *data, size_t *data_size, void **ptr);

	virtual void complete(struct MHD_Connection *connection, void **con_cls,
			enum MHD_RequestTerminationCode toe);

	int _request(struct MHD_Connection *connection, const char *url, const xic::QuestPtr& q);
};


struct ContentPolicy: public Policy
{
	bool stripPrefix;
	std::set<std::string> allowedPaths;

	std::string getName(const char *url);
	bool allowed(const std::string& name);
};

struct StoragePolicy: public ContentPolicy
{
	xic::ProxyPtr agentPrx;

	virtual int process(struct MHD_Connection *connection, const char *url, 
			const char *method, const char *version,
			const char *data, size_t *data_size, void **ptr);

	virtual void complete(struct MHD_Connection *connection, void **con_cls,
			enum MHD_RequestTerminationCode toe);
};

struct ThumbnailPolicy: public ContentPolicy
{
	enum PathFormat
	{
		PFMT_DIR,
		PFMT_TAIL,
	};

	xic::ProxyPtr memcachePrx;
	xic::ProxyPtr photoPrx;
	PathFormat pathFormat;

	virtual int process(struct MHD_Connection *connection, const char *url, 
			const char *method, const char *version,
			const char *data, size_t *data_size, void **ptr);

	virtual void complete(struct MHD_Connection *connection, void **con_cls,
			enum MHD_RequestTerminationCode toe);
};


class PolicyManager: public XRefCount
{
	typedef std::map<std::string, PolicyPtr> PolicyMap;
	PolicyMap _policyMap;
public:
	PolicyManager(const xic::EnginePtr& engine, const std::string& file);
	virtual ~PolicyManager();

	PolicyPtr getPolicy(const std::string& url);
};


#endif
