/*
 * BPF program for simple-kv
 *
 * Author: etm2131@columbia.edu
 */
#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>
#include "simplekvspec.h"

/* Mask to prevent out of bounds memory access */
#define KEY_MASK RNG_KEYS - 1

char LICENSE[] SEC("license") = "GPL";

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

static __inline unsigned int process_leaf(struct bpf_imposter *context, struct RangeQuery *query, Node *node) {
    key__t first_key = query->flags & RNG_BEGIN_EXCLUSIVE ? query->range_begin + 1 : query->range_begin;
    unsigned int end_inclusive = query->flags & RNG_END_INCLUSIVE;
    
    /* Iterate over keys in leaf node */
    int *i = &query->_node_key_ix;
    for(;;) {
        /* Iterate over keys in leaf node */
        for (; *i < NODE_CAPACITY && query->len < RNG_KEYS; ++(*i)) {
            if (node->key[*i & KEY_MASK] > query->range_end ||
                    (node->key[*i & KEY_MASK] == query->range_end && end_inclusive == 0)) {
                /* All done; set state and return 0 */
                mark_range_query_complete(query);
                context->done = 1;
                return 0;
            }
            /* Retrieve value for this key */
            if (node->key[*i & KEY_MASK] >= first_key) {
                /* Set up the next resubmit to read the value */
                query->_state = RNG_READ_VALUE;
                context->next_addr[0] = decode(value_base(node->ptr[*i & KEY_MASK]));
                context->size[0] = BLK_SIZE;
                return 0;
            }
        }

        /* Three conditions: Either the query buff is full, or we inspected all keys, or both */

        /* Check end condition of outer loop */
        if (query->len == RNG_KEYS) {
            /* Query buffer is full; need to suspend and return */
            context->done = 1;
            query->range_begin = query->kv[(query->len - 1) & KEY_MASK].key;
            query->flags |= RNG_BEGIN_EXCLUSIVE;
            if (*i < NODE_CAPACITY) {
                /* This node still has values we should inspect */
                return 0;
            }

            /* Need to look at next node */
            if (node->next == 0) {
                /* No next node, so we're done */
                mark_range_query_complete(query);
            } else {
                query->_resume_from_leaf = node->next;
                query->_node_key_ix = 0;
            }
            return 0;
        } else if (node->next == 0) {
            /* Still have room in query buf, but we've read the entire index */
            mark_range_query_complete(query);
            return 0;
        }

        /*
         * Query buff isn't full, so we inspected all keys in this node
         * and need to get the next node.
         */
        query->_resume_from_leaf = node->next;
        query->_node_key_ix = 0;
        context->next_addr[0] = node->next;
        context->size[0] = BLK_SIZE;
        return 0;
    }
}

static __inline unsigned int process_value(struct bpf_imposter *context, struct RangeQuery *query) {
    int *i = &query->_node_key_ix;
    unsigned long offset = value_offset(query->_current_node.ptr[*i & KEY_MASK]);
    memcpy(query->kv[query->len & KEY_MASK].value, context->data + offset, sizeof(val__t));
    query->len += 1;
    *i += 1;
    query->_state = RNG_RESUME;
    return process_leaf(context, query, &query->_current_node);
}

static __inline unsigned int traverse_index(struct bpf_imposter *context, struct RangeQuery *query, Node *node) {
    if (node->type == LEAF) {
        return process_leaf(context, query, node);
    }

    /* Grab the next node in the traversal */
    context->next_addr[0] = decode(nxt_node(query->range_begin, node));
    context->size[0] = BLK_SIZE;
    return 0;
}

SEC("oliver_agg")
unsigned int oliver_agg_func(struct bpf_imposter *context) {
    struct RangeQuery *query = (struct RangeQuery*) context->scratch;
    Node *node = (Node *) context->data;

    switch (query->_state) {
        case RNG_TRAVERSE:
            return traverse_index(context, query, node);
        case RNG_RESUME:
            return process_leaf(context, query, node);
        case RNG_READ_VALUE:
            return process_value(context, query);
        default:
            return -1;
    }
}
