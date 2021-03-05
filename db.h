#ifndef DB_H
#define DB_H

#include <stddef.h>

// Data-level information
typedef unsigned long meta__t;
typedef unsigned long key__t;
typedef unsigned char val__t;
typedef unsigned long ptr__t;

#define META_SIZE sizeof(meta__t)
#define KEY_SIZE sizeof(key__t)
#define VAL_SIZE sizeof(val__t)
#define PTR_SIZE sizeof(ptr__t)
#define BLK_SIZE 512

// Node-level information
#define INTERNAL 0
#define LEAF 1

#define NODE_CAPACITY (BLK_SIZE - 2 * META_SIZE) / (KEY_SIZE + PTR_SIZE)
#define LOG_CAPACITY  (BLK_SIZE - 1 * META_SIZE) / (KEY_SIZE + VAL_SIZE)
#define FANOUT NODE_CAPACITY

typedef struct _Node {
    meta__t num;
    meta__t type;
    key__t key[NODE_CAPACITY];
    ptr__t ptr[NODE_CAPACITY];
} Node;

typedef struct _Log {
    meta__t num;
    key__t key[LOG_CAPACITY];
    val__t val[LOG_CAPACITY];
} Log;

// Database-level information
#define IDX_PATH "/mydata/db.index"
#define LOG_PATH "/mydata/db.log"

size_t total_node;
size_t *layer_cap;

int load(size_t layer_num);

int run(size_t request_num);

val__t get(key__t k);

ptr__t next_node(key__t k, ptr__t p);

Node read_node(ptr__t p);

int prompt_help();

void logistic(size_t layer_num);

#endif