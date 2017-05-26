##
##
##

EXE = XiProxy

OBJS = XiProxy.o RevServant.o BigServant.o XiServant.o ProxyConfig.o \
	RCache.o Dlog.o LCache.o Quickie.o lz4codec.o \
	MCache.o Memcache.o MClient.o MOperation.o \
	Redis.o RedisGroup.o RedisClient.o RedisOp.o \
	MyMethodTab.o


CXXFLAGS = -g -Wall -O2

CPPFLAGS = -I. -I../include -I../knotty

LIBS = -rdynamic -pthread -L../lib -L../knotty/lib -lutil -lxic -ldlog -lxs -lrt


all: $(EXE)


$(EXE): $(OBJS)
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -o $(EXE) $^ $(LIBS)

clean:
	$(RM) $(EXE) $(OBJS)

