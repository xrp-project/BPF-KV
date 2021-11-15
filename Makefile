CC = gcc
CFLAGS = -Wall -pthread -D_GNU_SOURCE
LDFLAGS = -pthread

all: db nndb nnndb simple_retrieve

db: db.c db.h

nndb: nndb.c nndb.h

nnndb: nnndb.c nnndb.h

simple_retrieve: simple_retrieve.c db.h db_types.h

.PHONY: clean
clean:
	rm -rf db nndb nnndb simple_retrieve
