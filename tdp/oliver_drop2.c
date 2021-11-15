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

char LICENSE[] SEC("license") = "GPL";

static __inline int key_exists(unsigned long const key, Node *node) {
    /* Safety: NULL is never passed for node, but mr. verifier doesn't know that */
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


SEC("oliver_pass")
unsigned int oliver_pass_func(struct bpf_imposter *context) {
    struct Query *query = (struct Query *) context->scratch;
    Node *node = (Node *) context->data;

    /* Three cases:
     *
     * 1. We've found the log offset in the previous iteration and are
     *    now reading the value into the query result.
     *
     * 2. We've found a leaf node and need to a) verify the key exists and 2)
     *    get the log offset and make one more resubmission.
     *
     * 3. We're in an internal node and need to keep traversing the B+ tree
     */

    /* Case 1: read value into query result */
    bpf_printk("simplekv-bpf: entered\n");
    if (query->found) {
        bpf_printk("simplekv-bpf: case 1 - value found\n");

        ptr__t offset = decode(query->value_ptr) & (BLK_SIZE - 1);
        memcpy(query->value, context->data + offset, sizeof(query->value));
        context->done = 1;
        return 0;
    }

    /* Case 2: verify key & submit read for block containing value */
    if (node->type == LEAF) {
        bpf_printk("simplekv-bpf: case 2 - verify key & get last block\n");

        query->reached_leaf = 1;
        if (!key_exists(query->key, node)) {
            query->found = 0;
            context->done = 1;

            bpf_printk("simplekv-bpf: key doesn't exist\n");
            return 0;
        }
        query->found = 1;
        query->value_ptr = nxt_node(query->key, node);
        /* Need to submit a request for base of the block containing our offset */
        ptr__t base = decode(query->value_ptr) & ~(BLK_SIZE - 1);
        context->next_addr[0] = encode(base);
        context->size[0] = BLK_SIZE;

        bpf_printk("simplekv-bpf: nextaddr = %0lx\n", context->next_addr[0]);
        return 0;
    }

    /* Case 3: at an internal node, keep going */
    bpf_printk("simplekv-bpf: case 3 - internal node\n");
    context->next_addr[0] = encode(nxt_node(query->key, node));
    bpf_printk("simplekv-bpf: nextaddr = %0lx\n", context->next_addr[0]);
    context->size[0] = BLK_SIZE;
    return 0;
}
