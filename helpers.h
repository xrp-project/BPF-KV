#ifndef HELPERS_H
#define HELPERS_H

#include <errno.h>
#include <stdlib.h>
#include <stdio.h>

#include "db_types.h"

long lookup_bpf(int db_fd, struct Query *query);

void checked_pread(int fd, void *buf, size_t size, long offset);

ptr__t nxt_node(unsigned long key, Node *node);

int key_exists(unsigned long const key, Node const *node);

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

#endif /* HELPERS_H */