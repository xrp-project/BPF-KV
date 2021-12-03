CC = gcc
CFLAGS = -Wall -pthread -D_GNU_SOURCE -g -O0 -Wunused
LDFLAGS = -pthread


all: simplekv


simplekv: simplekv.c helpers.c simplekv.h helpers.h db_types.h


.PHONY: clean
clean:
	rm -rf simplekv


.PHONY: clean
copy:
	cp simplekv ${HOME}/kv-test
