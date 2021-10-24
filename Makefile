CC = gcc
CFLAGS = -Wall -pthread -D_GNU_SOURCE
LDFLAGS = -pthread

all: db nndb nnndb

db: db.c db.h

nndb: nndb.c nndb.h

nnndb: nnndb.c nnndb.h

.PHONY: clean
clean:
	rm -rf db nndb nnndb
