#ifndef RevServant_h_
#define RevServant_h_

#include "xic/Engine.h"
#include <string>

class RevServant: public xic::Servant
{
protected:
	xic::EnginePtr _engine;
	std::string _service;
	xstr_t _origin;
	int _revision;
	bool _serviceChanged;
	time_t _start_time;

public:
	RevServant(const xic::EnginePtr& engine, const std::string& service, int revision)
		: _engine(engine), _service(service), _revision(revision), _serviceChanged(false)
	{
		_start_time = _engine->time();
		xstr_cxx(&_origin, _service);
		ssize_t pound = xstr_find_char(&_origin, 0, '#');
		if (pound >= 0)
		{
			_origin.len = pound;
			_serviceChanged = true;
		}
	}

	const std::string& service() const 	{ return _service; }
	int revision() const			{ return _revision; }

	virtual void getInfo(xic::VDictWriter& dw);
};
typedef XPtr<RevServant> RevServantPtr;


#endif

