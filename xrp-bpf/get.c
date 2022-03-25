/*
 * BPF program for simple-kv
 *
 * Author: etm2131@columbia.edu
 */
#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>
#include "simplekvspec.h"

#ifndef NULL
#define NULL 0
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

/* State flags */
#define AT_VALUE 1

static __inline void set_context_next_index(struct bpf_xrp *context, struct ScatterGatherQuery *query) {
    query->current_index += 1;
    query->state_flags = 0;
    if (query->current_index >= query->n_keys || query->current_index >= SG_KEYS) {
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
unsigned int oliver_agg_func(struct bpf_xrp *context) {
    struct ScatterGatherQuery *query = (struct ScatterGatherQuery*) context->scratch;
    Node *node = (Node *) context->data;
    int *curr_idx = &query->current_index;

    /* Three cases:
     *
     * 1. We've found the log offset in the previous iteration and are
     *    now reading the value into the query result. If there are more
     *    keys to process, start again at the node.
     *
     * 2. We've found a leaf node and need to a) verify the key exists and 2)
     *    get the log offset and make one more resubmission to read the value.
     *    If the keys is missing, but there are more keys to process, start again
     *    at the root.
     *
     * 3. We're in an internal node and need to keep traversing the B+ tree
     */

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
