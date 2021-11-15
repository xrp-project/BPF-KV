#ifndef DB_TYPES_H
#define DB_TYPES_H


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

/* struct used to communicate with BPF function via scratch buffer */
struct Query {
    /* everything is a long to make debugging in gdb easier */
    long found;
    long reached_leaf;
    long prev_node;
    key__t key;
    val__t value;
    /*
    union {
        key__t key;
        val__t value;
    };
    */
    ptr__t value_ptr;
};

#endif /* DB_TYPES_H */

