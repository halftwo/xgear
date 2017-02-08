#ifndef Response_h_
#define Response_h_

#include <microhttpd.h>


void add_http2xic_header(MHD_Response *response);


int respond_ok(MHD_Connection *con);

int respond_not_found(MHD_Connection *con);

int respond_forbidden(MHD_Connection *con);

int respond_range_not_satisfiable(MHD_Connection *con);

int respond_not_modified(MHD_Connection *con);

int respond_internal_server_error(MHD_Connection *con);

int respond_bad_request(MHD_Connection *con);


#endif
