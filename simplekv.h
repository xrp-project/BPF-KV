#ifndef OLIVER_DB_H
#define OLIVER_DB_H

#include <stddef.h>
#include <stdio.h>
#include <pthread.h>
#include <sys/time.h>
#include <stdlib.h>
#include <errno.h>

#include "db_types.h"

// Database-level information
#define LOG_PATH "./oliver_log"
#define LOAD_MODE 0
#define RUN_MODE 1
#define FILE_MASK ((ptr__t)1 << 63)

size_t worker_num;
size_t total_node;
size_t *layer_cap;
key__t max_key;
Node *cache;
size_t cache_cap;

typedef struct {
    size_t op_count;
    size_t index;
    int db_handler;
    size_t timer;
    int log_handler;
    int use_xrp;
} WorkerArg;

int get_handler(char *db_path, int flag);
int get_log_handler(int flag);

ptr__t is_file_offset(ptr__t ptr) {
    return ptr & FILE_MASK;
}

ptr__t encode(ptr__t ptr) {
    return ptr | FILE_MASK;
}

ptr__t decode(ptr__t ptr) {
    return ptr & (~FILE_MASK);
}

int load(size_t layer_num, char *db_path);

int run(char *db_path, size_t layer_num, size_t request_num, size_t thread_num, int use_xrp, size_t cache_level);

void *subtask(void *args);

void build_cache(int db_fd, size_t layer_num, size_t cache_level);

void read_node(ptr__t ptr, Node *node, int db_handler);

void read_log(ptr__t ptr, Log *log, int db_handler);

int initialize(size_t layer_num, int mode, char *db_path);

void initialize_workers(WorkerArg *args, size_t total_op_count, char *db_path, int use_xrp);

void start_workers(pthread_t *tids, WorkerArg *args);

void terminate_workers(pthread_t *tids, WorkerArg *args);

int terminate(void);

int _get_leaf_containing(int database_fd, key__t key, Node *node, ptr__t *node_offset);
int get_leaf_containing(int database_fd, key__t key, Node *node);

long lookup_key_userspace(int db_fd, struct Query *query);

int compare_nodes(Node *x, Node *y);

void print_node(ptr__t ptr, Node *node);

typedef int(*key_iter_action)(int idx, Node *node, void *state);

int iterate_keys(char *filename, int levels, long start_key, long end_key,
                 key_iter_action fn, void *fn_state);

void free_globals(void);

int submit_range_query(struct RangeQuery *query, int db_fd, int use_xrp);

#endif /* OLIVER_DB_H */