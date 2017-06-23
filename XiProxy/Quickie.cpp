#include "Quickie.h"
#include "BigServant.h"
#include "xslib/hseq.h"
#include "xslib/crc.h"
#include "xslib/ScopeGuard.h"
#include <vector>

xic::MethodTab::PairType Quickie::_funpairs[] = {
#define CMD(X)	{ #X, XIC_METHOD_CAST(Quickie, X) },
	QUICKIE_CMDS
#undef CMD
};
xic::MethodTab Quickie::_funtab(_funpairs, XS_ARRCOUNT(_funpairs));

Quickie::Quickie(const BigServantPtr& bigsrv)
	: ServantI(&_funtab), _bigsrv(bigsrv)
{
}

Quickie::~Quickie()
{
}

XIC_METHOD(Quickie, time)
{
	time_t now;
	struct tm tm;
	char buf[32];
	xic::AnswerWriter aw;

	time(&now);
	aw.param("time", now);

	gmtime_r(&now, &tm);
	strftime(buf, sizeof(buf), "%Y%m%d-%H%M%S", &tm);
	aw.param("utc", buf);

	localtime_r(&now, &tm);
	strftime(buf, sizeof(buf), "%Y%m%d-%H%M%S", &tm);
	aw.param("local", buf);

	return aw;
}

XIC_METHOD(Quickie, sink)
{
	return xic::ONEWAY_ANSWER;
}

static void dec_xref_quest(void *cookie, void *buf)
{
	xic::Quest* q = static_cast<xic::Quest*>(cookie);
	q->xref_dec();
}

XIC_METHOD(Quickie, echo)
{
	xic::AnswerPtr answer = xic::Answer::create();
	answer->setStatus(xic::AnswerWriter::NORMAL);
	xstr_t xs = quest->args_xstr();
	quest->xref_inc();
	rope_append_external(answer->args_rope(), xs.data, xs.len, dec_xref_quest, quest.get());
	return answer;
}

XIC_METHOD(Quickie, hseq)
{
	xic::QuestReader qr(quest);
	std::vector<xstr_t> buckets;
	qr.wantBlobSeq("buckets", buckets);
	size_t size = buckets.size();

	std::vector<int> weights;
	qr.getIntSeq("weights", weights);
	bool weighted = (weights.size() >= size);

	size_t num = qr.getInt("num");
	if (num == 0)
		num = size;
	else if (num > size)
		num = size;

	uint32_t keyhash = qr.getInt("keyhash");
	if (!keyhash)
	{
		xstr_t key = qr.getBlob("key");
		if (key.len)
			keyhash = crc32_checksum(key.data, key.len);
	}

	uint32_t keymask = qr.getInt("keymask");
	if (keymask)
		keyhash &= keymask;

	std::vector<hseq_bucket_t> bs(size);
	for (size_t i = 0; i < size; ++i)
	{
		bs[i].identity = buckets[i].data;
		bs[i].idlen = buckets[i].len;
		bs[i].weight = weighted ? weights[i] : 1;
	}

	hseq_t *hs = hseq_create(&bs[0], size);
	ON_BLOCK_EXIT(hseq_destroy, hs);

	std::vector<int> seqs(num);
	hseq_hash_sequence(hs, keyhash, &seqs[0], num);

	xic::AnswerWriter aw;
	xic::VListWriter lw = aw.paramVList("seqs");
	for (size_t i = 0; i < seqs.size(); ++i)
	{
		lw.v(seqs[i]);
	}

	return aw;
}

XIC_METHOD(Quickie, salvo)
{
	return _bigsrv->salvo(quest, current);
}

