#ifndef DB_H
#define DB_H

// Data-level information
typedef unsigned long m_type;
typedef unsigned long k_type;
typedef unsigned char v_type;
typedef unsigned long p_type;

#define META_SIZE sizeof(m_type)
#define KEY_SIZE sizeof(k_type)
#define VAL_SIZE sizeof(v_type)
#define PTR_SIZE sizeof(p_type)
#define BLK_SIZE 512

// Node-level information
#define INTERNAL 0
#define LEAF 1

#define NODE_CAPACITY (BLK_SIZE - 2 * META_SIZE) / (KEY_SIZE + PTR_SIZE)
#define LOG_CAPACITY  (BLK_SIZE - 1 * META_SIZE) / (KEY_SIZE + VAL_SIZE)

typedef struct _Node {
    m_type num;
    m_type type;
    k_type key[NODE_CAPACITY];
    p_type ptr[NODE_CAPACITY];
} Node;

typedef struct _Log {
    m_type num;
    k_type key[LOG_CAPACITY];
    v_type val[LOG_CAPACITY];
} Log;

// Database-level information
#define DB_PATH "/mydata/kv.store"

int load(unsigned int num);

int run(unsigned int num);

v_type get(k_type k);

p_type next_node(k_type k, p_type p);

Node read_node(p_type p);

int prompt_help();

#endif