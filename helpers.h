#ifndef HELPERS_H
#define HELPERS_H

#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <alloca.h>
#include <stdint.h>

#include "db_types.h"

#define SYS_IMPOSTER_PREAD64 445

#define aligned_alloca(align, size)     (((uintptr_t) alloca((size) + (align) - 1) + ((align) - 1)) & ~ (uintptr_t) ((align) - 1));

long lookup_bpf(int db_fd, struct Query *query);

void checked_pread(int fd, void *buf, size_t size, long offset);

ptr__t nxt_node(unsigned long key, Node *node);

int key_exists(unsigned long key, Node const *node);

int _get_leaf_containing(int database_fd, key__t key, Node *node, ptr__t *node_offset);
int get_leaf_containing(int database_fd, key__t key, Node *node);

static inline long strtol_or_exit(char *str, char *fail_msg) {
    char *endptr = NULL;
    errno = 0;
    long result = strtol(str, &endptr, 10);
    if ((endptr != NULL && *endptr != '\0') || errno != 0) {
        fprintf(stderr, "%s", fail_msg);
        exit(1);
    }
    return result;
}

static inline unsigned long strtoul_or_exit(char *str, char *fail_msg) {
    char *endptr = NULL;
    errno = 0;
    unsigned long result = strtoul(str, &endptr, 10);
    if ((endptr != NULL && *endptr != '\0') || errno != 0) {
        fprintf(stderr, "%s", fail_msg);
        exit(1);
    }
    return result;
}

int compare_nodes(Node *x, Node *y);

#endif /* HELPERS_H */