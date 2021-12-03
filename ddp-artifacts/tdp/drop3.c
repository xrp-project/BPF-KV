#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>
#include "simplekvspec.h"

#ifndef memcpy
#define memcpy(dest, src, n)   __builtin_memcpy((dest), (src), (n))
#endif

struct halfnode {
	unsigned long num;
	unsigned long type;
	unsigned long key[31];
};

struct restnode {
	unsigned long val[31];
};

SEC("ddp_next")
unsigned long ddp_pass_func(struct ddp_key dk)
{
	//unsigned long i = 0;
	//unsigned long key = dk.key;
	struct restnode hn;
	memcpy(&hn, dk.data[sizeof(struct halfnode)], sizeof(struct restnode));
	return hn.val[dk.result];
}
