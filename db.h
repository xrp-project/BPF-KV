#ifndef DB_H
#define DB_H

#include <stdlib.h>
#include <unistd.h>
#include <stddef.h>
#include <stdio.h>
#include <pthread.h>
#include <sys/time.h>
#include "spdk/stdinc.h"
#include "spdk/thread.h"
#include "spdk/bdev.h"
#include "spdk/env.h"
#include "spdk/event.h"
#include "spdk/log.h"
#include "spdk/string.h"

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
typedef struct {
	struct spdk_bdev *bdev;
	struct spdk_bdev_desc *bdev_desc;
	struct spdk_io_channel *bdev_io_channel;
	struct spdk_bdev_io_wait_entry bdev_io_wait;
} Context;

typedef struct {
    void *buff;
    size_t *counter;
    size_t target;
    uint64_t offset;
    Context *context;
} Request;

typedef struct {
    size_t op_count;
    size_t index;
    int db_handler;
    size_t timer;
} WorkerArg;

#define BDEV_NAME "NVMe2n1"
#define DB_PATH "./db.storage"
#define LOAD_MODE 0
#define RUN_MODE 1
#define FILE_MASK ((ptr__t)1 << 63)

char*  db_mode = NULL;
size_t layer_cnt   = 0;
size_t request_cnt = 0;
size_t thread_cnt  = 0;

size_t worker_num;
size_t total_node;
size_t *layer_cap;
key__t max_key;
int db;
Context *db_ctx;
Node *cache;
size_t cache_cap;

int get_handler(int flag);

Context *init_context();

Request *init_request(Context *ctx, size_t *c, size_t t, size_t o, void *b);

ptr__t is_file_offset(ptr__t ptr) {
    return ptr & FILE_MASK;
}

ptr__t encode(ptr__t ptr) {
    return ptr | FILE_MASK;
}

ptr__t decode(ptr__t ptr) {
    return ptr & (~FILE_MASK);
}

int load(void *argv);

int run(void *argv);

void *subtask(void *args);

void build_cache(size_t layer_num);

int get(key__t key, val__t val, int db_handler);

ptr__t next_node(key__t key, Node *node);

void read_node(ptr__t ptr, Node *node, Context *ctx, size_t *counter, size_t target);

void read_log(ptr__t ptr, Node *node, Context *ctx, size_t *counter, size_t target);

int retrieve_value(ptr__t ptr, val__t val, int db_handler);

void prompt_help(void);

void initialize(size_t layer_num, int mode);

void initialize_workers(WorkerArg *args, size_t total_op_count);

void start_workers(pthread_t *tids, WorkerArg *args);

void terminate_workers(pthread_t *tids, WorkerArg *args);

int terminate();

int compare_nodes(Node *x, Node *y);

void print_node(ptr__t ptr, Node *node);

void bdev_event_cb(enum spdk_bdev_event_type type, struct spdk_bdev *bdev, void *event_ctx);

int parse_arg(int ch, char *arg);

void write_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg);

void read_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg);

void spdk_write(void *arg);

#endif
