
CXX=g++
CXXFLAGS=-std=c++11 -ggdb -Wall -Wextra -pedantic -Werror -Wnon-virtual-dtor -I../dependencies/include
SERVEROBJS= server-main.o logger.o SurfStoreServer.o
CLIENTOBJS= client-main.o logger.o SurfStoreClient.o

default: ssd ss

%.o: %.c
	$(CXX) $(CXXFLAGS) -c -o $@ $<

ss: $(CLIENTOBJS)
	$(CXX) $(CXXFLAGS) -o ss $(CLIENTOBJS) -L../dependencies/lib -pthread -lrpc

ssd: $(SERVEROBJS)
	$(CXX) $(CXXFLAGS) -o ssd $(SERVEROBJS) -L../dependencies/lib -pthread -lrpc

.c.o:
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f ss ssd *.o
