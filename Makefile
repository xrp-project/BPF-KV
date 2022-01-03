CC = gcc
CFLAGS = -Wall -pthread -D_GNU_SOURCE -Wunused
LDFLAGS = -pthread


all: simplekv bpf loader


simplekv: simplekv.c simplekv.h db_types.h helpers.o range.o parse.o create.o get.o

helpers.o: helpers.c helpers.h db_types.h

range.o: range.c range.h db_types.h parse.h db_types.h simplekv.h helpers.h

parse.o: parse.c parse.h helpers.h

create.o: create.c create.h parse.h db_types.h simplekv.h

get.o : get.c get.h db_types.h parse.h simplekv.h

loader: xrp_loader.c
	clang xrp_loader.c -o xrp_loader -lbpf

.PHONY: bpf
bpf:
	make -C xrp-bpf -f Makefile

.PHONY: clean
clean:
	rm -rf simplekv *.o
