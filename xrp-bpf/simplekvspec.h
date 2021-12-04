#ifndef SKV_H
#define SKV_H

#include "../db_types.h"

// Node-level information
#define INTERNAL 0
#define LEAF 1

#define FILE_MASK ((ptr__t)1 << 63)

#ifndef memcpy
#define memcpy(dest, src, n)   __builtin_memcpy((dest), (src), (n))
#endif

// struct
struct ddp_key{
	unsigned char data[512];
	unsigned long key;	
};

#ifdef VERBOSE
#define dbg_print(...) bpf_printk(__VA_ARGS__)

static __inline void print_query(struct Query *q) {
    dbg_print("struct Query {\n");

    dbg_print("\tfound = %ld\n", q->found);
    dbg_print("\treached_leaf = %ld\n", q->state_flags & REACHED_LEAF);
    dbg_print("\tkey = %ld\n", q->key);
    dbg_print("\tvalue = %s\n", q->value);
    dbg_print("\tvalue_ptr = %ld\n", q->value_ptr);

    dbg_print("}\n");
}

static __inline void print_node(Node *node) {
    dbg_print("struct Node {\n");

    dbg_print("\tnum = %ld\n", node->num);
    dbg_print("\ttype = %ld\n", node->type);
    dbg_print("\tkey[0] = %ld\n", node->key[0]);
    dbg_print("\tkey[30] = %ld\n", node->key[NODE_CAPACITY - 1]);
    dbg_print("\tptr[0] = 0x%lx\n", node->ptr[0]);
    dbg_print("\tptr[30] = 0x%lx\n", node->ptr[NODE_CAPACITY - 1]);

    dbg_print("}\n");
}
#else
#define dbg_print(...)
#endif



#endif
