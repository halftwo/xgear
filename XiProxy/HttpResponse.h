#ifndef HttpResponse_h_
#define HttpResponse_h_

#include <microhttpd.h>


MHD_Result http_respond_bad_request(MHD_Connection *con, const char *msg="");

MHD_Result http_respond_forbidden(MHD_Connection *con, const char *msg="");

MHD_Result http_respond_method_not_allowed(MHD_Connection *con, const char *msg="");


MHD_Result http_respond_internal_server_error(MHD_Connection *con, const char *msg="");



#endif
