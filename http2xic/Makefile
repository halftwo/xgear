#
#

# This project depends on GNU libmicrohttpd


CFLAGS = -g -Wall -rdynamic

CXXFLAGS = -g -Wall -rdynamic

CPPFLAGS = -I. -I../knotty -D__BUILD_TIME=`date +%s`

LIBS = -pthread -Wl,-static -L../knotty/lib -lxic -ldlog -lxs -Wl,-call_shared -lmicrohttpd -lrt 

OBJS = Policy.o HttpHandler.o Response.o Http2XicServant.o

EXES = http2xic

all: ${OBJS} ${EXES}

$(EXES): $(OBJS)

.c.o:
	$(CC) -c $(CFLAGS) $(CPPFLAGS) $(INC) -o $@ $<

.cpp.o:
	$(CXX) -c $(CXXFLAGS) $(CPPFLAGS) $(INC) -o $@ $<

.c:
	$(CC) $(CFLAGS) $(CPPFLAGS) $(INC) -o $@ $^ $(LIBS)

.cpp:
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) $(INC) -o $@ $^ $(LIBS)

clean:
	$(RM) $(OBJS) $(EXES)


