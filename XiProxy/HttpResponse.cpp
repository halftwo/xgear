#include "HttpResponse.h"
#include "xslib/xstr.h"


static MHD_Result respond(MHD_Connection *con, int code, const char *msg)
{
	MHD_Response *response = MHD_create_response_from_buffer(strlen(msg), (void *)msg, MHD_RESPMEM_PERSISTENT);
	MHD_add_response_header(response, MHD_HTTP_HEADER_CONTENT_TYPE, "text/plain");
	MHD_Result ret = MHD_queue_response(con, code, response);
	MHD_destroy_response(response);
	return ret;
}


MHD_Result http_respond_bad_request(MHD_Connection *con, const char *msg)
{
	return respond(con, MHD_HTTP_BAD_REQUEST, msg);
}

MHD_Result http_respond_forbidden(MHD_Connection *con, const char *msg)
{
	return respond(con, MHD_HTTP_FORBIDDEN, msg);
}

MHD_Result http_respond_method_not_allowed(MHD_Connection *con, const char *msg)
{
	return respond(con, MHD_HTTP_METHOD_NOT_ALLOWED, msg);
}


MHD_Result http_respond_internal_server_error(MHD_Connection *con, const char *msg)
{
	return respond(con, MHD_HTTP_INTERNAL_SERVER_ERROR, msg);
}

