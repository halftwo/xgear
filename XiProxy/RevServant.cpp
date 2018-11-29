#include "RevServant.h"
#include "XiProxy.h"

void RevServant::getInfo(xic::VDictWriter& dw)
{
	char buf[32];

	dw.kv("service", _service);
	dw.kv("revision", _revision);
	dw.kv("birth_time", xp_get_time_str(_start_time, buf));
}

