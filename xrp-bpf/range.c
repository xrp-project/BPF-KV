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

/* Mask to prevent out of bounds memory access */
#define KEY_MASK (RNG_KEYS - 1)

char LICENSE[] SEC("license") = "GPL";

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

static __inline unsigned int process_leaf(struct bpf_xrp *context, struct RangeQuery *query, Node *node) {
    key__t first_key = query->flags & RNG_BEGIN_EXCLUSIVE ? query->range_begin + 1 : query->range_begin;
    unsigned int end_inclusive = query->flags & RNG_END_INCLUSIVE;
    
    /* Iterate over keys in leaf node */
    unsigned int *i = &query->_node_key_ix;
    for(;;) {
        /* Iterate over keys in leaf node */
        for (; *i < NODE_CAPACITY && query->len < RNG_KEYS; ++(*i)) {
            key__t curr_key = node->key[*i & KEY_MASK];
            if (curr_key > query->range_end || (curr_key == query->range_end && !end_inclusive)) {
                /* All done; set state and return 0 */
                mark_range_query_complete(query);
                context->done = 1;
                return 0;
            }
            /* Retrieve value for this key */
            if (curr_key >= first_key) {
                /* Set up the next resubmit to read the value */
                context->next_addr[0] = value_base(decode(node->ptr[*i & KEY_MASK]));
                context->size[0] = BLK_SIZE;

                key__t key = node->key[*i & KEY_MASK];
                query->kv[query->len & KEY_MASK].key = key;

                /* Fixup the begin range so that we don't try to grab the same key again */
                query->range_begin = key;
                query->flags |= RNG_BEGIN_EXCLUSIVE;

                query->_state = RNG_READ_VALUE;
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
                query->_state = RNG_READ_NODE;
            }
            /* Return to user since we marked context->done = 1 at the top of this if block */
            return 0;
        } else if (node->next == 0) {
            /* Still have room in query buf, but we've read the entire index */
            mark_range_query_complete(query);
            context->done = 1;
            return 0;
        }

        /*
         * Query buff isn't full, so we inspected all keys in this node
         * and need to get the next node.
         */
        query->_resume_from_leaf = node->next;
        query->_state = RNG_READ_NODE;
        query->_node_key_ix = 0;
        context->next_addr[0] = node->next;
        context->size[0] = BLK_SIZE;
        return 0;
    }
}

static __inline unsigned int process_value(struct bpf_xrp *context, struct RangeQuery *query) {
    unsigned int *i = &query->_node_key_ix;
    unsigned long offset = value_offset(decode(query->_current_node.ptr[*i & KEY_MASK]));

    if (query->agg_op == AGG_NONE) {
        memcpy(query->kv[query->len & KEY_MASK].value, context->data + offset, sizeof(val__t));
        query->len += 1;
    }
    else if (query->agg_op == AGG_SUM) {
        query->agg_value += *(long *) (context->data + offset);
    }

    /* TODO: This should be incremented, but not doing so does not affect correctness.
     *   For some reason, if we do increment, the verifier complains in `process_leaf` about
     *   access using the query->_node_key_ix variable to index into the node's key / ptr array.
     *   It seems like this shouldn't be an issue due to the loop invariant, but the verifier
     *   disagrees...
     */
    // *i += 1;
    query->_state = RNG_RESUME;
    return process_leaf(context, query, &query->_current_node);
}

static __inline unsigned int traverse_index(struct bpf_xrp *context, struct RangeQuery *query, Node *node) {
    if (node->type == LEAF) {
        query->_current_node = *node;
        return process_leaf(context, query, node);
    }

    /* Grab the next node in the traversal */
    context->next_addr[0] = decode(nxt_node(query->range_begin, node));
    context->size[0] = BLK_SIZE;
    return 0;
}

SEC("oliver_range")
unsigned int oliver_range_func(struct bpf_xrp *context) {
    struct RangeQuery *query = (struct RangeQuery*) context->scratch;
    Node *node = (Node *) context->data;

    switch (query->_state) {
        case RNG_TRAVERSE:
            return traverse_index(context, query, node);
        case RNG_READ_NODE:
            query->_current_node = *node;
            query->_state = RNG_RESUME;
            /* FALL THROUGH */
        case RNG_RESUME:
            return process_leaf(context, query, node);
        case RNG_READ_VALUE:
            return process_value(context, query);
        default:
            context->done = 1;
            return -1;
    }
}
