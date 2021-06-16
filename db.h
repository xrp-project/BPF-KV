#ifndef DB_H
#define DB_H

#include <stddef.h>
#include <stdio.h>
#include <pthread.h>
#include <sys/time.h>
#include <liburing.h>

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

// Database-level information
#define DB_PATH "/dev/nvme0n1"
#define LOAD_MODE 0
#define RUN_MODE 1
#define FILE_MASK ((ptr__t)1 << 63)
#define QUEUE_DEPTH 1

size_t layer_cnt;
size_t cache_layer = 3;
size_t worker_num;
size_t total_node;
size_t *layer_cap;
key__t max_key;
int db;
Node *cache;
size_t cache_cap;
pthread_mutex_t *val_lock;
size_t read_ratio;
size_t rmw_ratio;
struct io_uring global_ring;

typedef struct {
    size_t op_count;
    size_t index;
    int db_handler;
    size_t timer;
    struct io_uring local_ring;
    size_t counter;
} WorkerArg;

typedef struct {
    key__t key;
    ptr__t ofs;
    struct iovec vec;
    bool is_value;
    struct timeval start;
    WorkerArg *warg;
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

int load(size_t layer_num);

int run(size_t layer_num, size_t request_num, size_t thread_num);

void *subtask(void *args);

void build_cache(size_t layer_num);

int get(key__t key, val__t val, WorkerArg *r);

void update(key__t key, val__t val, int db_handler);

void read_modify_write(key__t key, val__t val, int db_handler);

ptr__t next_node(key__t key, Node *node);

Request *init_request(key__t key, WorkerArg *warg);

void read_node(ptr__t ptr, Node *node, int db_handler, struct io_uring *ring);

void read_log(ptr__t ptr, Log *log, int db_handler, struct io_uring *ring);

void read_complete(struct io_uring *ring, int is_node);

void traverse(ptr__t ptr, Request *req);

void traverse_complete(struct io_uring *ring);

void wait_for_completion(struct io_uring *ring, size_t *counter, size_t target);

void write_node(ptr__t ptr, Node *node, int db_handler, struct io_uring *ring);

void write_log(ptr__t ptr, Log *log, int db_handler, struct io_uring *ring);

void write_complete(struct io_uring *ring);

int retrieve_value(ptr__t ptr, val__t val, WorkerArg *r);

void update_value(ptr__t ptr, val__t val, int db_handler);

void read_modify_write_value(ptr__t ptr, val__t val, int db_handler);

int prompt_help();

void initialize(size_t layer_num, int mode);

void initialize_workers(WorkerArg *args, size_t total_op_count);

void start_workers(pthread_t *tids, WorkerArg *args);

void terminate_workers(pthread_t *tids, WorkerArg *args);

int terminate();

void print_node(ptr__t ptr, Node *node);

#endif
