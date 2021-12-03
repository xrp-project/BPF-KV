LLC ?= llc
CLANG ?= clang
CC ?= gcc

BPF_CFLAGS ?= -I$(LIBBPF_DIR)/build/usr/

all: oliver

oliver: oliver_drop2.o oliver_gather.o oliver_range.o

oliver_drop2.o: oliver_drop2.c simplekvspec.h ../db_types.h

oliver_gather.o: oliver_gather.c simplekvspec.h ../db_types.h

oliver_range.o: oliver_range.c simplekvspec.h ../db_types.h


%.o: %.c
	$(CLANG) -S \
	    -target bpf \
	    -D __BPF_TRACING__ \
	    $(BPF_CFLAGS) \
	    -Wall \
	    -Wno-unused-value \
	    -Wno-pointer-sign \
	    -Wno-compare-distinct-pointer-types \
	    -Werror \
	    -O2 -emit-llvm -c -g -o ${@:.o=.ll} $<
	$(LLC) -march=bpf -filetype=obj -o $@ ${@:.o=.ll}


.PHONY: clean
clean: 
	rm -rf *.o


.PHONY: copy
	cp oliver_drop2.o oliver_gather.o oliver_range.o ${HOME}/kv-test/
