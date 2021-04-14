#ifndef DB_H
#define DB_H

#include <stdlib.h>
#include <unistd.h>
#include <stddef.h>
#include <stdio.h>
#include <pthread.h>
#include <sys/time.h>
#include "spdk/stdinc.h"
#include "spdk/nvme.h"
#include "spdk/vmd.h"
#include "spdk/nvme_zns.h"
#include "spdk/env.h"

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
    key__t key;
    ptr__t ofs;
    void *buff;
    bool is_completed;
    bool is_value;
    struct spdk_nvme_ns	*ns;
    struct spdk_nvme_qpair *qpair;
    struct Request *next;
} Request;

typedef struct {
    size_t op_count;
    size_t index;
    struct spdk_nvme_qpair *qpair;
    size_t timer;
} WorkerArg;

#define BDEV_NAME "NVMe2n1"
#define DB_PATH "./db.storage"
#define LOAD_MODE 0
#define RUN_MODE 1
#define FILE_MASK ((ptr__t)1 << 63)

char*  db_mode = NULL;
size_t cache_layer = 3;
size_t layer_cnt   = 0;
size_t request_cnt = 0;
size_t thread_cnt  = 0;
size_t counter = 0;

size_t worker_num;
size_t total_node;
size_t *layer_cap;
key__t max_key;
Node *cache;
size_t cache_cap;
struct spdk_nvme_ctrlr *global_ctrlr = NULL;
struct spdk_nvme_ns	   *global_ns    = NULL;
struct spdk_nvme_qpair *global_qpair = NULL;

Request *init_request(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair, void *buff, key__t key);

ptr__t is_file_offset(ptr__t ptr) {
    return ptr & FILE_MASK;
}

ptr__t encode(ptr__t ptr) {
    return ptr | FILE_MASK;
}

ptr__t decode(ptr__t ptr) {
    return ptr & (~FILE_MASK);
}

int load();

int run();

void *subtask(void *args);

void build_cache(size_t layer_num);

int get(key__t key, val__t val, struct spdk_nvme_qpair *qpair, Request **pending_list);

ptr__t next_node(key__t key, Node *node);

void wait_for_completion(Request **list, struct spdk_nvme_qpair *qpair);

void add_pending_req(Request **list, Request *req);

void write_complete(void *arg, const struct spdk_nvme_cpl *completion);

void spdk_write(Request **list, Request *req, size_t lba, size_t nlba);

void read_complete(void *arg, const struct spdk_nvme_cpl *completion);

void spdk_read(Request **list, Request *req, size_t lba, size_t nlba, spdk_nvme_cmd_cb cb_fn);

// void read_node(ptr__t ptr, Node *node, Context *ctx, size_t *counter, size_t target);

// void read_log(ptr__t ptr, Node *node, Context *ctx, size_t *counter, size_t target);

void initialize(size_t layer_num, int mode);

void initialize_workers(WorkerArg *args, size_t total_op_count);

void start_workers(pthread_t *tids, WorkerArg *args);

void terminate_workers(pthread_t *tids, WorkerArg *args);

int terminate();

int compare_nodes(Node *x, Node *y);

void print_node(ptr__t ptr, Node *node);

void prompt_help();

void parse_args(int argc, char *argv[]);

bool probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
              struct spdk_nvme_ctrlr_opts *opts);

void attach_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
	           struct spdk_nvme_ctrlr *ctrlr, const struct spdk_nvme_ctrlr_opts *opts);

void cleanup();
#endif
