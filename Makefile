CC = gcc
CFLAGS = -Wall -pthread -D_GNU_SOURCE -g -O0 -Wunused
LDFLAGS = -pthread

all: skv-test

skv-test: simplekv
	cp simplekv ${HOME}/kv-test

db: db.c db.h

nndb: nndb.c nndb.h

nnndb: nnndb.c nnndb.h

simplekv: simplekv.c helpers.c simplekv.h helpers.h 

.PHONY: clean
clean:
	rm -rf db nndb simplekv
