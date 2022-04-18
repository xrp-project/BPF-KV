#ifndef DB_H
#define DB_H

#include <stddef.h>
#include <stdio.h>
#include <pthread.h>
#include <time.h>
#include <math.h>
#include "uring.h"

#include <linux/bpf.h>
#include <linux/lirc.h>
#include <linux/input.h>
#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <fcntl.h>

// Data-level information
typedef unsigned long meta__t;
typedef unsigned long key__t;
typedef unsigned char val__t[64];
typedef unsigned long ptr__t;

#define META_SIZE sizeof(meta__t)
#define KEY_SIZE sizeof(key__t)
#define VAL_SIZE sizeof(val__t)
#define PTR_SIZE sizeof(ptr__t)
#define BLK_SIZE 512
#define BLK_SIZE_LOG 9

// Node-level information
#define INTERNAL 0
#define LEAF 1

#define NODE_CAPACITY ((BLK_SIZE - 2 * META_SIZE) / (KEY_SIZE + PTR_SIZE))
#define LOG_CAPACITY  ((BLK_SIZE) / (VAL_SIZE))
#define FANOUT NODE_CAPACITY

typedef struct _Node {
    meta__t num;
    meta__t type;
    key__t key[NODE_CAPACITY];
    ptr__t ptr[NODE_CAPACITY];
} Node;

typedef struct _Log {
    val__t val[LOG_CAPACITY];
} Log;

#define SG_KEYS 32

struct MaybeValue {
    char found;
    val__t value;
};

struct ScatterGatherQuery {
    ptr__t root_pointer;
    ptr__t value_ptr;
    unsigned int state_flags;
    int current_index;
    int n_keys;
    key__t keys[SG_KEYS];
    struct MaybeValue values[SG_KEYS];
};

// Database-level information
#define DB_PATH "/dev/nvme0n1"
#define LOAD_MODE 0
#define RUN_MODE 1
#define FILE_MASK ((ptr__t)1 << 63)
#define QUEUE_DEPTH 512
#define MIN_BATCH_SIZE 64

#define CLOCK_TYPE CLOCK_MONOTONIC

size_t layer_cnt;
size_t cache_layer = 3;
size_t worker_num;
size_t request_cnt;
size_t total_node;
size_t *layer_cap;
key__t max_key;
int db;
Node *cache;
size_t cache_cap;
pthread_mutex_t *val_lock;
size_t read_ratio;
size_t rmw_ratio;
size_t req_per_sec;
int bpf_fd;

typedef struct {
    size_t op_count;
    size_t index;
    int db_handler;
    size_t timer;
    struct submitter local_ring;
    size_t finished;
    size_t issued;
    size_t *histogram;
} WorkerArg;

typedef struct {
    key__t key;
    struct iovec vec;
    uint8_t *scratch_buffer;
    struct timespec start;
    WorkerArg *warg;
    int index;
} Request;

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

void init_ring(struct submitter *s);

int load(size_t layer_num);

int run();

size_t get_nano(struct timespec ts);

void add_nano_to_timespec(struct timespec *x, size_t nano);

void *subtask(void *args);

void build_cache(size_t layer_num);

void *print_status(void *args);

ptr__t next_node(key__t key, Node *node);

Request *init_request(key__t key, WorkerArg *warg);

void pread_node(ptr__t ptr, Node *node, int db_handler);

void pread_log(ptr__t ptr, Log *log, int db_handler);

void traverse(ptr__t ptr, Request *req);

int traverse_complete(struct submitter *s);

void pwrite_node(ptr__t ptr, Node *node, int db_handler);

void pwrite_log(ptr__t ptr, Log *log, int db_handler);

int prompt_help();

void initialize(size_t layer_num, int mode);

void initialize_workers(WorkerArg *args, size_t total_op_count);

void start_workers(pthread_t *tids, WorkerArg *args);

void terminate_workers(pthread_t *tids, WorkerArg *args);

int terminate();

void print_node(ptr__t ptr, Node *node);

int bpf(int cmd, union bpf_attr *attr, unsigned int size);

int load_bpf_program(char *path);

#endif
