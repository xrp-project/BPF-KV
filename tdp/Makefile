LLC ?= llc
CLANG ?= clang
CC ?= gcc

BPF_CFLAGS ?= -I$(LIBBPF_DIR)/build/usr/

all: drop.o drop2.o
#all: drop.o drop2.o drop3.o

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

clean: 
	rm -rf *.o
