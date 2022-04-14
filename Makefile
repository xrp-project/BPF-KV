CC = gcc
CFLAGS = -Wall -D_GNU_SOURCE -Wunused
LDLIBS = -pthread -lbpf -lm


all: simplekv bpf


simplekv: simplekv.c simplekv.h db_types.h helpers.o range.o parse.o create.o get.o

helpers.o: helpers.c helpers.h db_types.h

range.o: range.c range.h db_types.h parse.h db_types.h simplekv.h helpers.h

parse.o: parse.c parse.h helpers.h

create.o: create.c create.h parse.h db_types.h simplekv.h

get.o : get.c get.h db_types.h parse.h simplekv.h

.PHONY: bpf
bpf:
	make -C xrp-bpf -f Makefile

.PHONY: clean
clean:
	rm -rf simplekv *.o
	make -C xrp-bpf -f Makefile clean
