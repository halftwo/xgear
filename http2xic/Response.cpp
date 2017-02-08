#include "Response.h"
#include "version.h"
#include "xslib/xstr.h"

static MHD_Response *ok;
static MHD_Response *not_found;
static MHD_Response *forbidden;
static MHD_Response *range_not_satisfiable;
static MHD_Response *not_modified;
static MHD_Response *internal_server_error;
static MHD_Response *bad_request;

void add_http2xic_header(MHD_Response *response)
{
	MHD_add_response_header(response, "HTTP2XIC", HTTP2XIC_VERSION);
}

static int _init()
{
	static xstr_t ok_xs = XSTR_CONST("");
	static xstr_t not_found_xs = XSTR_CONST("Location Not Found :-(\n");
	static xstr_t forbidden_xs = XSTR_CONST("Location Forbidden :-(\n");
	static xstr_t range_not_satisfiable_xs = XSTR_CONST("Invalid Range :-(\n");
	static xstr_t not_modified_xs = XSTR_CONST("Not Modified :-(\n");
	static xstr_t internal_server_error_xs = XSTR_CONST("Internal Server Error :-(\n");
	static xstr_t bad_request_xs = XSTR_CONST("Bad Request :-(\n");

#define CREATE(X)	\
	X = MHD_create_response_from_buffer(X##_xs.len, X##_xs.data, MHD_RESPMEM_PERSISTENT);	\
	add_http2xic_header(X);									\
	MHD_add_response_header(X, MHD_HTTP_HEADER_CONTENT_TYPE, "text/plain");			\

	CREATE(ok);
	CREATE(not_found);
	CREATE(forbidden);
	CREATE(range_not_satisfiable);
	CREATE(not_modified);
	CREATE(internal_server_error);
	CREATE(bad_request);

#undef CREATE
	return 1;
}

static int _dummy = _init();

int respond_ok(MHD_Connection *con)
{
	return MHD_queue_response(con, MHD_HTTP_OK, ok);
}

int respond_not_found(MHD_Connection *con)
{
	return MHD_queue_response(con, MHD_HTTP_NOT_FOUND, not_found);
}

int respond_forbidden(MHD_Connection *con)
{
	return MHD_queue_response(con, MHD_HTTP_FORBIDDEN, forbidden);
}

int respond_range_not_satisfiable(MHD_Connection *con)
{
	return MHD_queue_response(con, MHD_HTTP_REQUESTED_RANGE_NOT_SATISFIABLE, range_not_satisfiable);
}

int respond_not_modified(MHD_Connection *con)
{
	return MHD_queue_response(con, MHD_HTTP_NOT_MODIFIED, not_modified);
}

int respond_internal_server_error(MHD_Connection *con)
{
	return MHD_queue_response(con, MHD_HTTP_INTERNAL_SERVER_ERROR, internal_server_error);
}

int respond_bad_request(MHD_Connection *con)
{
	return MHD_queue_response(con, MHD_HTTP_BAD_REQUEST, bad_request);
}

