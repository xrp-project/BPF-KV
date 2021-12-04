CC = gcc
CFLAGS = -Wall -pthread -D_GNU_SOURCE -g -O0 -Wunused
LDFLAGS = -pthread


all: simplekv


simplekv: simplekv.c simplekv.h db_types.h helpers.o range.o parse.o create.o get.o

helpers.o: helpers.c helpers.h db_types.h

range.o: range.c range.h db_types.h parse.h db_types.h simplekv.h helpers.h

parse.o: parse.c parse.h

create.o: create.c create.h parse.h db_types.h simplekv.h

get.o : get.c get.h db_types.h parse.h simplekv.h

.PHONY: clean
clean:
	rm -rf simplekv *.o


.PHONY: clean
copy:
	cp simplekv ${HOME}/kv-test
