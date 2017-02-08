#include "lz4codec.h"
#include "xslib/lz4.h"
#include "xslib/xnet.h"
#include "xslib/xxhash.h"

#define MAGIC		0x1a7fb4f5
#define	HEADER_SIZE	sizeof(struct myzip_header)	// should be 12

struct myzip_header
{
	uint32_t magic;		// 0x1a7fb4f5
	uint32_t length;	// length of the uncompressed data
	uint32_t hash;		// xxhash of the compressed data
};

int attempt_lz4_zip(ostk_t *ostk, const xstr_t& in, xstr_t& out)
{
	if (in.len < 48)
		return -1;
	else if (in.len > ZIP_MAX_SIZE)
		return -2;

	void *sentry = ostk_alloc(ostk, 0);
	int len = LZ4_compressBound(in.len) + HEADER_SIZE;
	char *buf = (char *)ostk_alloc(ostk, len);
	len = LZ4_compress((char *)in.data, buf + HEADER_SIZE, in.len);
	if (len >= 0)
	{
		len += HEADER_SIZE;
		if (len < in.len * ZIP_SIZE_PERCENT)
		{
			uint32_t hash = XXH32(buf + HEADER_SIZE, len - HEADER_SIZE, 0);
			struct myzip_header *hdr = (struct myzip_header *)buf;
			hdr->magic = xnet_m32(MAGIC);
			hdr->length = xnet_m32(in.len);
			hdr->hash = xnet_m32(hash);

			out.data = (unsigned char *)buf;
			out.len = len;
			ostk_free(ostk, out.data + out.len);
			return 0;
		}
	}

	ostk_free(ostk, sentry);
	return -3;
}

int attempt_lz4_unzip(ostk_t* ostk, const xstr_t& in, xstr_t& out)
{
	if (in.len <= (ssize_t)HEADER_SIZE)
		return -1;

	struct myzip_header hdr;
	memcpy(&hdr, in.data, HEADER_SIZE);
	xnet_msb32(&hdr.magic);
	xnet_msb32(&hdr.length);
	xnet_msb32(&hdr.hash);
	if (hdr.magic != MAGIC)
		return -2;

	if (hdr.length < 0 || hdr.length > ZIP_MAX_SIZE)
		return -4;

	char *ibuf = (char *)in.data + HEADER_SIZE;
	int ilen = in.len - HEADER_SIZE;
	uint32_t hash = XXH32(ibuf, ilen, 0);
	if (hash != hdr.hash)
		return -3;

	void *sentry = ostk_alloc(ostk, 0);
	char *obuf = (char *)ostk_alloc(ostk, hdr.length);
	int olen = LZ4_decompress_safe(ibuf, obuf, ilen, hdr.length);
	if (olen == (int)hdr.length)
	{
		out.data = (unsigned char *)obuf;
		out.len = olen;
		return 0;
	}

	ostk_free(ostk, sentry);
	return -5;
}


