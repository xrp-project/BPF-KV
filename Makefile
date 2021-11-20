CC = gcc
CFLAGS = -Wall -pthread -D_GNU_SOURCE
LDFLAGS = -pthread

all: sr oliver_db

db: db.c db.h

nndb: nndb.c nndb.h

nnndb: nnndb.c nnndb.h

oliver_db: oliver_db.c oliver_db.h

simple_retrieve: simple_retrieve.c db.h db_types.h helpers.c helpers.h

sr: simple_retrieve
	cp simple_retrieve /home/evan/kv-test/

.PHONY: clean
clean:
	rm -rf db nndb nnndb simple_retrieve oliver_db
