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

/* State Flags for BPF Functions */
#define REACHED_LEAF 1

/* struct used to communicate with BPF function via scratch buffer */
struct Query {
    /* everything is a long to make debugging in gdb easier */
    key__t key;
    long found;
    long state_flags;

    val__t value;
    /* Used to store file offset to the value once we've located it via a leaf node */
    ptr__t value_ptr;

    /* Current node (possibly a leaf being used to process results) and its parent */
    Node current_node;
    ptr__t current_parent;
};

static inline struct Query new_query(long key) {
    struct Query query = {
        .key = key,
        .found = 0,
        .state_flags = 0,
        .value = { 0 },
        .value_ptr = 0,
        .current_node = { 0 },
        .current_parent = 0
    };
    return query;
}

/* 
| LEAF NODE |
     \    \
      \    \
       \    --------------    
       | HEAP BLOCK|      \
	                  | HEAP BLOCK | 

*/

#endif /* DB_TYPES_H */

