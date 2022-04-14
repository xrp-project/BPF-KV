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
#define LOAD_MODE 0
#define RUN_MODE 1

#define CREATE_CMD "create"
#define RANGE_CMD "range"
#define GET_CMD "get"

extern size_t worker_num;
extern size_t total_node;
extern size_t *layer_cap;
extern key__t max_key;
extern Node *cache;
extern size_t cache_cap;

typedef struct {
    size_t op_count;
    size_t index;
    int db_handler;
    size_t timer;
    int use_xrp;
    int bpf_fd;
    size_t *latency_arr;
} WorkerArg;

int get_handler(char *db_path, int flag);

int run(char *db_path, size_t layer_num, size_t request_num, size_t thread_num, int use_xrp, int bpf_fd, size_t cache_level);

void *subtask(void *args);

void build_cache(int db_fd, size_t layer_num, size_t cache_level);

void read_node(ptr__t ptr, Node *node, int db_handler);

void read_log(ptr__t ptr, Log *log, int db_handler);

int initialize(size_t layer_num, int mode, char *db_path);

void initialize_workers(WorkerArg *args, size_t total_op_count, char *db_path, int use_xrp, int bpf_fd);

void start_workers(pthread_t *tids, WorkerArg *args);

void terminate_workers(pthread_t *tids, WorkerArg *args);

int terminate(void);

int compare_nodes(Node *x, Node *y);

void print_node(ptr__t ptr, Node *node);

void free_globals(void);

#endif /* OLIVER_DB_H */