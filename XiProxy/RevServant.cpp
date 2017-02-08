#include "RevServant.h"

void RevServant::getInfo(xic::VDictWriter& dw)
{
	struct tm tm;
	char time_str[32];

	localtime_r(&_start_time, &tm);
	strftime(time_str, sizeof(time_str), "%Y%m%d-%H%M%S", &tm);

	dw.kv("service", _service);
	dw.kv("revision", _revision);
	dw.kv("birth_time", time_str);
}

