#include <string.h>
#include <unistd.h>
#include <argp.h>
#include <fcntl.h>
#include <time.h>

#include "range.h"
#include "parse.h"
#include "db_types.h"
#include "simplekv.h"
#include "helpers.h"

static void print_query_results(struct RangeQuery *query) {
    if (query->agg_op == AGG_NONE) {
        char buf[sizeof(val__t) + 1] = { 0 };
        buf[sizeof(val__t)] = '\0';
        for (int i = 0; i < query->len; ++i) {
            memcpy(buf, query->kv[i].value, sizeof(val__t));
            char *trimmed = buf;
            while (isspace(*trimmed)) {
                ++trimmed;
            }
            fprintf(stdout, "%s\n", trimmed);
        }
    }
    else {
        fprintf(stdout, "%ld\n", query->agg_value);
    }
}

int do_range_cmd(int argc, char *argv[], struct ArgState *as) {
    struct RangeArgs ra = { .requests = 1 };
    parse_range_opts(argc, argv, &ra);
    if (ra.range_size && ra.range_size - 1 > calculate_max_key(as->layers)) {
        fprintf(stderr, "range size exceeds database size\n");
        exit(1);
    }

    /* Load BPF program */
    int bpf_fd = -1;
    if (ra.xrp) {
        bpf_fd = load_bpf_program("xrp-bpf/range.o");
    }

    /**
     * Range Query
     *
     * Runs the range query requested at the command line and dumps the values
     * (as ASCII with whitespace trimmed) to stdout separated by a newline.
     */
    struct RangeQuery query = { .agg_op = ra.agg_op };

    /* Open the database */
    int db_fd = get_handler(as->filename, O_RDONLY);

    /* Retrieve values in range and print */
    struct timespec start, stop, l_start, l_stop;
    long total_time = 0, total_latency = 0;
    clock_gettime(CLOCK_REALTIME, &start);

    /* Used to generate random ranges */
    srandom(start.tv_nsec ^ start.tv_sec);
    max_key = calculate_max_key(as->layers);

    for (long i = 0; i < ra.requests; ++i) {
        if (ra.range_size) {
            ra.range_begin = random() % (max_key + 2 - ra.range_size);
            ra.range_end = ra.range_begin + ra.range_size;
        }
        set_range(&query, ra.range_begin, ra.range_end, 0);

        for (;;) {
            clock_gettime(CLOCK_REALTIME, &l_start);
            int rv = submit_range_query(&query, db_fd, ra.xrp, bpf_fd);
            clock_gettime(CLOCK_REALTIME, &l_stop);

            total_latency += NS_PER_SEC * (l_stop.tv_sec - l_start.tv_sec) + (l_stop.tv_nsec - l_start.tv_nsec);

            if (rv != 0) {
                exit(rv);
            }
            if (ra.dump_flag) {
                print_query_results(&query);
            }
            if (prep_range_resume(&query)) {
                break;
            }
        }
    }
    clock_gettime(CLOCK_REALTIME, &stop);
    total_time = NS_PER_SEC * (stop.tv_sec - start.tv_sec) + (stop.tv_nsec - start.tv_nsec);

    /* Dump results */
    double throughput = ((double) ra.requests / (double) total_time) * NS_PER_SEC; // ops/sec
    double latency = (double) total_latency / (double) ra.requests / US_PER_NS;
    unsigned long range_size = ra.range_size ? ra.range_size : ra.range_end - ra.range_begin;
    printf("Range Size: %lu, Average throughput: %f op/s latency: %f usec\n", range_size, throughput, latency);

    close(db_fd);
    return 0;
}

int submit_range_query(struct RangeQuery *query, int db_fd, int use_xrp, int bpf_fd) {
    char *scratch = (char *) aligned_alloca(0x1000, 0x1000);
    memset(scratch, 0, 0x1000);
    /* XRP code path */
    if (use_xrp) {
        char *buf = (char *) aligned_alloca(0x1000, 0x1000);

        struct RangeQuery *scratch_query = (struct RangeQuery*) scratch;
        *scratch_query = *query;
        long ret = syscall(SYS_READ_XRP, db_fd, buf, BLK_SIZE, query->_resume_from_leaf, bpf_fd, scratch);
        *query = *scratch_query;
        if (ret > 0) {
            return 0;
        }
        return (int) ret;
    }

    /* User space code path */
    Node *node = (Node *) aligned_alloca(BLK_SIZE, sizeof(Node));

    if (query->_state == RNG_RESUME) {
        checked_pread(db_fd, (void *) node, sizeof(Node), (long) query->_resume_from_leaf);
    } else {
        ptr__t node_offset = 0;
        if (_get_leaf_containing(db_fd, query->range_begin, node, ROOT_NODE_OFFSET, &node_offset) != 0) {
            fprintf(stderr, "Failed getting leaf node for key %ld\n", query->range_begin);
            return 1;
        }
        query->_resume_from_leaf = node_offset;
    }

    key__t first_key = query->flags & RNG_BEGIN_EXCLUSIVE ? query->range_begin + 1 : query->range_begin;
    unsigned long end_inclusive = query->flags & RNG_END_INCLUSIVE;
    for(;;) {
        /* Iterate over keys in leaf node */
        unsigned int i = 0;
        for (; i < NODE_CAPACITY && query->len < RNG_KEYS; ++i) {
            if (node->key[i] > query->range_end || (node->key[i] == query->range_end && !end_inclusive)) {
                /* All done; set state and return 0 */
                mark_range_query_complete(query);
                return 0;
            }
            /* Retrieve value for this key */
            if (node->key[i] >= first_key) {
                /*
                 * TODO (etm): We perform one read for each value since our hypothetical assumption is
                 *   that the values are stored in a random heap and not in sorted order (which they actually are).
                 *   We should confirm that this is the correct assumption to make and also keep in mind that
                 *   from user space there reads will be cached in the BIO layer unless we do direct IO using
                 *   IO_URING or another facility.
                 *
                 *   This should be discussed before we run performance benchmarks.
                 */

                /* This fiddiling around is necessary since we're using O_DIRECT */
                ptr__t ptr = decode(node->ptr[i]);
                checked_pread(db_fd, scratch, BLK_SIZE, (long) value_base(ptr));
                /* What we do next depends on the type of opp we're doing */
                if (query->agg_op == AGG_NONE) {
                    memcpy(query->kv[query->len].value, scratch + value_offset(ptr), sizeof(val__t));

                    query->kv[query->len].key = node->key[i];
                    query->len += 1;
                }
                else if (query->agg_op == AGG_SUM) {
                    query->agg_value += *(long*) (scratch + value_offset(ptr));
                }
            }
        }

        /* Three conditions: Either the query buff is full, or we inspected all keys, or both */

        /* Check end condition of outer loop */
        if (query->len == RNG_KEYS) {
            /* Query buffer is full; need to suspend and return */
            query->range_begin = query->kv[query->len - 1].key;
            query->flags |= RNG_BEGIN_EXCLUSIVE;
            if (i < NODE_CAPACITY) {
                /* This node still has values we should inspect */
                return 0;
            }

            /* Need to look at next node */
            if (node->next == 0) {
                /* No next node, so we're done */
                mark_range_query_complete(query);
            } else {
                query->_resume_from_leaf = node->next;
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
        checked_pread(db_fd, (void *) node, sizeof(Node), (long) node->next);
    }
}

/* Simple function that prints the key; for use with `iterate_keys` */
int iter_print(int idx, Node *node, void *state) {
    printf("%ld\n", node->key[idx]);
    return 0;
}

/* Dumps all keys by scanning across the leaf nodes
 *
 * NB: This function is generic, but unfortunately since C doesn't support
 * real generics it isn't monomorphized. Keep this in mind for benchmarks.
 * Maybe we should use C++ or inline the [key_iter_action].
 **/
int iterate_keys(char *filename, int levels, key__t start_key, key__t end_key,
                 key_iter_action fn, void *fn_state) {
    if (levels < 2) {
        fprintf(stderr, "Too few levels for dump-keys operation\n");
        exit(1);
    }

    int db_fd = open(filename, O_RDONLY);
    if (db_fd < 0) {
        perror("failed to open database");
        exit(1);
    }

    Node node = { 0 };
    if (get_leaf_containing(db_fd, start_key, &node, ROOT_NODE_OFFSET) != 0) {
        fprintf(stderr, "Failed dumping keys\n");
        exit(1);
    }
    printf("Dumping keys in B+ tree\n");
    for (;;) {
        for (unsigned int i = 0; i < NODE_CAPACITY; ++i) {
            if (node.key[i] >= end_key) {
                break;
            }
            int status = fn(i, &node, fn_state);
            if (status != 0) {
                return status;
            }
        }
        if (node.next == 0) {
            break;
        }
        checked_pread(db_fd, &node, sizeof(Node), (long) node.next);
    }
    close(db_fd);
    return 0;
}
