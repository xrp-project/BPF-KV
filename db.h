#ifndef DB_H
#define DB_H

#include <stddef.h>
#include <stdio.h>
#include <pthread.h>
#include <sys/time.h>
#include "db_types.h"

// Node-level information
#define INTERNAL 0
#define LEAF 1

// Database-level information
#define DB_PATH "/dev/nvme0n1"
#define LOAD_MODE 0
#define RUN_MODE 1
#define FILE_MASK ((ptr__t)1 << 63)

size_t worker_num;
size_t total_node;
size_t *layer_cap;
key__t max_key;
int db;
Node *cache;
size_t cache_cap;

typedef struct {
    size_t op_count;
    size_t index;
    int db_handler;
    size_t timer;
} WorkerArg;

int get_handler(int flag);

ptr__t is_file_offset(ptr__t ptr) {
    return ptr & FILE_MASK;
}

ptr__t encode(ptr__t ptr) {
    return ptr | FILE_MASK;
}

ptr__t decode(ptr__t ptr) {
    return ptr & (~FILE_MASK);
}

int load(size_t layer_num);

int run(size_t layer_num, size_t request_num, size_t thread_num);

void *subtask(void *args);

void build_cache(size_t layer_num);

int get(key__t key, val__t val, int db_handler);

ptr__t next_node(key__t key, Node *node);

void read_node(ptr__t ptr, Node *node, int db_handler);

void read_log(ptr__t ptr, Log *log, int db_handler);

int retrieve_value(ptr__t ptr, val__t val, int db_handler);

int prompt_help();

void initialize(size_t layer_num, int mode);

void initialize_workers(WorkerArg *args, size_t total_op_count);

void start_workers(pthread_t *tids, WorkerArg *args);

void terminate_workers(pthread_t *tids, WorkerArg *args);

int terminate();

int compare_nodes(Node *x, Node *y);

void print_node(ptr__t ptr, Node *node);

#endif
