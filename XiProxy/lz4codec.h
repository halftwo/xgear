#ifndef lz4codec_h_
#define lz4codec_h_

#include "xslib/ostk.h"
#include "xslib/xstr.h"

#define ZIP_THRESHOLD		864	// some arbitrary value
#define ZIP_SIZE_PERCENT	0.95
#define ZIP_MAX_SIZE 		(1024*1024*16-1)


/* 0 for success
 * negative number on error
 */

int attempt_lz4_zip(ostk_t *ostk, const xstr_t& in, xstr_t& out);

int attempt_lz4_unzip(ostk_t* ostk, const xstr_t& in, xstr_t& out);


#endif
