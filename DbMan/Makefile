#
#
#
.SUFFIXES: .cpp

CFLAGS = -g -Wall -O2

CXXFLAGS = -g -Wall -O2

CXXCPPFLAGS = -I. -I../include -I../knotty -I/usr/local/include


EXES = DbMan IdMan

OBJS_DBPOOL = DBSetting.o MySQLdb.o DBCluster.o QueryJob.o kind4vbs.o CallerKind.o SQLResult.o

LIB_DBPOOL = libdbpool.a


MYSQL_LIBS = -lmariadbclient

LIBS = -rdynamic -pthread -L../lib -L../knotty/lib -ldlog -lxic -lxs $(MYSQL_LIBS) -lrt




all: $(LIB_DBPOOL) $(EXES)

$(LIB_DBPOOL): $(OBJS_DBPOOL)
	$(AR) -rcs $@ $(OBJS_DBPOOL)

DbMan: DbMan.o DbManServant.o $(LIB_DBPOOL)
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -o $@ $^ $(LIBS) 

IdMan: IdMan.o IdManServant.o $(LIB_DBPOOL)
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -o $@ $^ $(LIBS) 


.cpp.o:
	$(CXX) -c $(CXXFLAGS) $(CXXCPPFLAGS) -o $@ $<

clean:
	$(RM) $(EXES) 
	$(RM) $(OBJS_DBPOOL) $(LIB_DBPOOL) *.o


