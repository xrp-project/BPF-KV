#ifndef SKV_H
#define SKV_H

#include "../db_types.h"

// Node-level information
#define INTERNAL 0
#define LEAF 1

#define FILE_MASK ((ptr__t)1 << 63)

ptr__t encode(ptr__t ptr) {
    return ptr | FILE_MASK;
}

ptr__t decode(ptr__t ptr) {
    return ptr & (~FILE_MASK);
}

// struct
struct ddp_key{
	unsigned char data[512];
	unsigned long key;	
};
#endif
