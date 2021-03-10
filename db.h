#ifndef DB_H
#define DB_H

#include <stddef.h>
#include <stdio.h>

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

typedef struct _CacheEntry {
    ptr__t ptr;
    Node node;
} CacheEntry;

// Database-level information
#define DB_PATH "/mydata/db.storage"
#define LOAD_MODE 0
#define RUN_MODE 1

size_t total_node;
size_t *layer_cap;
key__t max_key;
FILE *db;
CacheEntry *cache;
size_t cache_cap;

int load(size_t layer_num);

int run(size_t layer_num, size_t request_num);

void build_cache(size_t layer_num);

int is_cached(ptr__t ptr, Node *node);

int get(key__t key, val__t val);

ptr__t next_node(key__t key, ptr__t ptr, Node *node);

void read_node(ptr__t ptr, Node *node);

void read_log(ptr__t ptr, Log *log);

int retrieve_value(ptr__t ptr, val__t val);

int prompt_help();

void initialize(size_t layer_num, int mode);

int terminate();

int compare_nodes(Node *x, Node *y);

void print_node(ptr__t ptr, Node *node);

#endif