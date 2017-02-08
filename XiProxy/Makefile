##
##
##

EXE = XiProxy

OBJS = XiProxy.o RevServant.o BigServant.o XiServant.o ProxyConfig.o \
	RCache.o Dlog.o LCache.o Quickie.o lz4codec.o \
	MCache.o Memcache.o MClient.o MOperation.o \
	Redis.o RedisGroup.o RedisClient.o RedisOp.o


CXXFLAGS = -g -Wall -rdynamic -O2

CPPFLAGS = -I. -I../include -I../knotty

LIBS = -L../lib -L../knotty/lib -lutil -lxic -ldlog -lxs -lrt -lpthread


all: $(EXE)


$(EXE): $(OBJS)
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -o $(EXE) $^ $(LIBS)

clean:
	$(RM) $(EXE) $(OBJS)

