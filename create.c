#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

#include "create.h"
#include "parse.h"
#include "db_types.h"
#include "simplekv.h"


int do_create_cmd(int argc, char *argv[], struct ArgState *as) {
    parse_create_opts(argc, argv);
    return load(as->layers, as->filename);
}

/* Create a new database on disk at [db_path] with [layer_num] layers */
int load(size_t layer_num, char *db_path) {
    printf("Load the database of %lu layers\n", layer_num);
    int db = initialize(layer_num, LOAD_MODE, db_path);
    int const MB = (1<<20);

    // 1. Load the index
    Node * const node_begin;
    int node_entries = (MB * 10) / sizeof(Node);
    if (posix_memalign((void **)&node_begin, 512, node_entries * sizeof(Node))) {
        perror("posix_memalign failed");
        close(db);
        free_globals();
        exit(1);
    }
    /*
    Disk layout:
    B+ tree nodes, each with 31 keys and 31 associated block offsets to other nodes
    Nodes are written by level in order, so, the root is first, followed by all nodes on the second level.
    Since each node has pointers to 31 other nodes, fanout is 31
    | 0  - 1  2  3  4 ... 31 - .... | ### LOG DATA ### |

    Leaf nodes have pointers into the log data, which is appended as a "heap" in the same file
    at the end of the B+tree. Once we reach a leaf node, we scan through its keys and if one matches
    the key we need, we read the offset into the heap and can retrieve the value.
    */
    ptr__t next_pos = 1;
    long next_node_offset = 1;
    Node *node = node_begin;
    Node * const node_buf_end = node_begin + node_entries;
    for (size_t i = 0; i < layer_num; i++) {
        size_t extent = max_key / layer_cap[i], start_key = 0;
        printf("layer %lu extent %lu\n", i, extent);
        for (size_t j = 0; j < layer_cap[i]; ++j, ++next_node_offset) {
            node->type = (i == layer_num - 1) ? LEAF : INTERNAL;
            size_t sub_extent = extent / NODE_CAPACITY;
            if (j == layer_cap[i] - 1) {
                /* Last node in this level */
                node->next = 0;
            } else {
                /* Pointer to the next node in this level; used for efficient scans */
                node->next = next_node_offset * sizeof(Node);
            }
            for (size_t k = 0; k < NODE_CAPACITY; k++) {
                node->key[k] = start_key + k * sub_extent;
                node->ptr[k] = node->type == INTERNAL ?
                               encode(next_pos   * BLK_SIZE) :
                               encode(total_node * BLK_SIZE + (next_pos - total_node) * VAL_SIZE);
                next_pos++;
            }

            node += 1;
            if (node == node_buf_end) {
                ssize_t write_size = node_entries * sizeof(Node);
                ssize_t bytes_written = write(db, node_begin, write_size);
                if (bytes_written != write_size) {
                    fprintf(stderr, "failure: partial write of index node\n");
                    exit(1);
                }
                node = node_begin;
            }
            start_key += extent;
        }
    }
    /* Write any remaining node buffer */
    if (node > node_begin) {
        ssize_t write_size = (node - node_begin) * sizeof(Node);
        ssize_t bytes_written = write(db, node_begin, write_size);
        if (bytes_written != write_size) {
            fprintf(stderr, "failure: partial write of index node\n");
            exit(1);
        }
    }
    free(node_begin);

    // 2. Load the value log
    Log * const log_begin;
    int const log_entries = (MB * 10) / sizeof(Log);
    if (posix_memalign((void **)&log_begin, 512, log_entries * sizeof(Log))) {
        perror("posix_memalign failed");
        close(db);
        free_globals();
        exit(1);
    }
    printf("Writing value heap\n");
    Log *log = log_begin;
    Log * const log_end = log_begin + log_entries;
    for (size_t i = 0; i < max_key; i += LOG_CAPACITY) {
        for (size_t j = 0; j < LOG_CAPACITY; j++) {
            sprintf((char *) log->val[j], "%63lu", i + j);
        }
        ++log;
        if (log == log_end) {
            ssize_t write_size = log_entries * sizeof(Log);
            ssize_t bytes_written = write(db, log_begin, write_size);
            if (bytes_written != write_size) {
                fprintf(stderr, "failure: partial write of log data\n");
                exit(1);
            }
            log = log_begin;
        }
    }
    /* Write any remaining entries */
    if (log != log_begin) {
        ssize_t write_size = (log - log_begin) * sizeof(Log);
        ssize_t bytes_written = write(db, log_begin, write_size);
        if (bytes_written != write_size) {
            fprintf(stderr, "failure: partial write of log data\n");
            exit(2);
        }
    }

    free(log_begin);
    close(db);
    return terminate();
}
