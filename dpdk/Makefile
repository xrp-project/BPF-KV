PKGCONF = pkg-config

DPDK_CFLAGS = $(shell $(PKGCONF) --cflags libdpdk)
DPDK_LDFLAGS = $(shell $(PKGCONF) --libs libdpdk)

BPFKV_CFLAGS = -I.. -D_GNU_SOURCE
BPFKV_LDFLAGS = -pthread -lbpf -lm

GPP = g++ -Wall -g -O3 -fno-omit-frame-pointer

all: .bin/userspace_server.out .bin/dpdk_server.out

.bin:
	mkdir -p .bin

# DPDK Server
.bin/dpdk_server.out: .bin dpdk_server.o server_helpers.o
	$(GPP) $(DPDK_CFLAGS) -o .bin/dpdk_server.out dpdk_server.o server_helpers.o ../get.o ../helpers.o ../parse.o ../simplekv.o $(DPDK_LDFLAGS) $(BPFKV_LDFLAGS) -lyaml-cpp

dpdk_server.o: dpdk_server.cpp
	$(GPP) $(DPDK_CFLAGS) $(BPFKV_CFLAGS) -c dpdk_server.cpp -o dpdk_server.o $(DPDK_LDFLAGS) $(BPFKV_LDFLAGS) -lyaml-cpp

# Userspace server

.bin/userspace_server.out: .bin userspace_server.o server_helpers.o
	$(GPP) -o .bin/userspace_server.out userspace_server.o server_helpers.o -lyaml-cpp

userspace_server.o: userspace_server.cpp
	$(GPP) -c userspace_server.cpp -o userspace_server.o

server_helpers.o: server_helpers.cpp server_helpers.h
	$(GPP) $(BPFKV_CFLAGS) -c server_helpers.cpp -o server_helpers.o -lyaml-cpp

clean:
	rm -rf *.bin
	rm *.o

