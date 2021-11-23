/*
 * BPF program for simple-kv
 *
 * Author: etm2131@columbia.edu
 */
#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>
#include "simplekvspec.h"

#ifndef memcpy
#define memcpy(dest, src, n)   __builtin_memcpy((dest), (src), (n))
#endif


#ifdef VERBOSE
#define dbg_print(...) bpf_printk(__VA_ARGS__)
#else
#define dbg_print(...)
#endif

char LICENSE[] SEC("license") = "GPL";

static __inline int key_exists(unsigned long const key, Node *node) {
    /* Safety: NULL is never passed for node, but mr. verifier doesn't know that */
    dbg_print("simplekv-bpf: key_exists entered\n");
    if (node == NULL)
        return -1;
    for (int i = 0; i < NODE_CAPACITY; ++i) {
        if (node->key[i] == key) {
            return 1;
        }
    }
    return 0;
}

static __inline ptr__t nxt_node(unsigned long key, Node *node) {
    /* Safety: NULL is never passed for node, but mr. verifier doesn't know that */
    dbg_print("simplekv-bpf: nxt_node entered\n");
    if (node == NULL)
        return -1;
    for (int i = 1; i < NODE_CAPACITY; ++i) {
        if (key < node->key[i]) {
            return node->ptr[i - 1];
        }
    }
    /* Key wasn't smaller than any of node->key[x], so take the last ptr */
    return node->ptr[NODE_CAPACITY - 1];
}

#ifdef VERBOSE
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
#endif

/* State flags */
#define AT_VALUE 1

static __inline void set_context_next_index(struct bpf_imposter *context, struct ScatterGatherQuery *query) {
    query->current_index += 1;
    query->state_flags = 0;
    if (query->current_index == SG_KEYS) {
        context->done = 1;
        context->next_addr[0] = 0;
        context->size[0] = 0;
    } else {
        context->next_addr[0] = query->root_pointer;
        context->size[0] = BLK_SIZE;
    }
}

/* Mask to prevent out of bounds memory access */
#define EBPF_CONTEXT_MASK SG_KEYS - 1

SEC("oliver_agg")
unsigned int oliver_agg_func(struct bpf_imposter *context) {
    struct ScatterGatherQuery *query = (struct ScatterGatherQuery*) context->scratch;
    Node *node = (Node *) context->data;
    int *curr_idx = &query->current_index;

    /* Case 1: read value into query result */
    dbg_print("simplekv-bpf: entered\n");
    if (query->state_flags & AT_VALUE) {
        dbg_print("simplekv-bpf: case 1 - value found\n");

        ptr__t offset = query->value_ptr & (BLK_SIZE - 1);
        struct MaybeValue *mv = &query->values[*curr_idx & EBPF_CONTEXT_MASK];
        mv->found = 1;
        memcpy(mv->value, context->data + offset, sizeof(val__t));

        set_context_next_index(context, query);
        return 0;
    }

    /* Case 2: verify key & submit read for block containing value */
    if (node->type == LEAF) {
        dbg_print("simplekv-bpf: case 2 - verify key & get last block\n");

        query->state_flags = REACHED_LEAF;
        if (!key_exists(query->keys[*curr_idx & EBPF_CONTEXT_MASK], node)) {
            dbg_print("simplekv-bpf: key doesn't exist\n");

            /* Skip this key */
            query->values[*curr_idx & EBPF_CONTEXT_MASK].found = 0;
            set_context_next_index(context, query);
            return 0;
        }
        query->state_flags = AT_VALUE;
        query->value_ptr = decode(nxt_node(query->keys[*curr_idx & EBPF_CONTEXT_MASK], node));
        /* Need to submit a request for base of the block containing our offset */
        ptr__t base = query->value_ptr & ~(BLK_SIZE - 1);
        context->next_addr[0] = base;
        context->size[0] = BLK_SIZE;
        return 0;
    }

    /* Case 3: at an internal node, keep going */
    dbg_print("simplekv-bpf: case 3 - internal node\n");
    context->next_addr[0] = decode(nxt_node(query->keys[*curr_idx & EBPF_CONTEXT_MASK], node));
    context->size[0] = BLK_SIZE;
    return 0;
}
