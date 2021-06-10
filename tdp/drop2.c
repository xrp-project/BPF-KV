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

SEC("ddp_pass")
unsigned long ddp_pass_func(struct ddp_key dk)
{
	unsigned long i = 0;
	unsigned long key = dk.key;
	// struct halfnode hn;
	// memcpy(&hn, dk.data, sizeof(struct halfnode));
	struct halfnode *hn = (struct halfnode *)dk.data;
	unsigned long f = hn->type == 1 ? 32 : 0;
	
	#pragma unroll
	for (i = 0; i < 31; i++) {
		if (key < hn->key[i]) {
			return i - 1 + f;
		}
	}
	return hn->num - 1 + f;
}
