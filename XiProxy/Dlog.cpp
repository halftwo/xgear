#include "Dlog.h"
#include "dlog/dlog.h"
#include "xslib/cstr.h"

Dlog::Dlog()
{
}

Dlog::~Dlog()
{
}

xic::AnswerPtr Dlog::process(const xic::QuestPtr& quest, const xic::Current& current)
{
	static const xstr_t _identity = XSTR_CONST("XP");

	if (xstr_equal_cstr(&quest->method(), "log"))
	{
		xic::QuestReader qr(quest);
		const xstr_t& identity = qr.getXstr("identity");
		const xstr_t& tag = qr.getXstr("tag");
		const xstr_t& locus = qr.getXstr("locus");
		const xstr_t& content = qr.getXstr("content");

		char buf[36];
		xstr_t id;
		if (identity.len)
		{
			char *p = buf, *end = buf + sizeof(buf);
			p = cstr_pcopyn(p, end, "XP:", 3);
			p = cstr_pcopyn(p, end, (char *)identity.data, identity.len);
			xstr_init(&id, (unsigned char *)buf, p < end ? p - buf : sizeof(buf)-1); 
		}
		else
		{
			id = _identity;
		}

		zdlog(&id, &tag, &locus, &content);
	}

	return xic::ONEWAY_ANSWER;
}

