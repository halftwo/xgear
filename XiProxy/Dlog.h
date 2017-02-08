#ifndef Dlog_h_
#define Dlog_h_

#include "xic/Engine.h"

class Dlog: public xic::Servant
{
public:
	Dlog();
	virtual ~Dlog();

	virtual xic::AnswerPtr process(const xic::QuestPtr& quest, const xic::Current& current);
};

#endif
