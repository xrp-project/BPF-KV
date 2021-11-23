/* This is a simple program that traverses the custom B+ tree structure
 * used by simple-kv.
 *
 * Author: etm2131@columbia.edu
 *
 * Usage:
 * ./simple_retrieve database_file key
 *
 */

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/file.h>
#include <errno.h>
#include <ctype.h>
#include <string.h>

#include "db.h"
#include "helpers.h"

/* Function using the same bit fiddling that we use in the BPF function */
void read_value_the_hard_way(int fd, char *retval, ptr__t ptr) {
    /* Base of the block containing our vale */
    ptr__t base = decode(ptr) & ~(BLK_SIZE - 1);
    printf("base: 0x%lx\n", base);
    char buf[BLK_SIZE];
    checked_pread(fd, buf, BLK_SIZE, (long) base);
    ptr__t offset = decode(ptr) & (BLK_SIZE - 1);
    memcpy(retval, buf + offset, sizeof(val__t));
}

/**
 * Traverses the B+ tree index and retrieves the value associated with
 * [key] from the heap, if [key] is in the database.
 * @param file_name
 * @param key
 * @return null terminated string containing the value on disk, or NULL if key not found
 */
char *grab_value(char *file_name, unsigned long const key, int use_xrp) {
    char *const retval = malloc(sizeof(val__t) + 1);
    if (retval == NULL) {
        perror("malloc");
        exit(1);
    }
    /* Ensure we have a null at the end of the string */
    retval[sizeof(val__t)] = '\0';

    /* Open the database */
    // TODO (etm): This is never closed
    int flags = O_RDONLY;
    if (use_xrp) {
    flags = flags | O_DIRECT;
    }
    int db_fd = open(file_name, flags);
    if (db_fd < 0) {
        perror("failed to open database");
        exit(1);
    }

    if (use_xrp) {
        struct Query query = new_query(key);
        long ret = lookup_bpf(db_fd, &query);

        if (ret < 0) {
            printf("reached leaf? %ld\n", query.state_flags);
            fprintf(stderr, "read xrp failed with code %d\n", errno);
            fprintf(stderr, "%s\n", strerror(errno));
            exit(errno);
        }
        if (query.found == 0) {
            printf("reached leaf? %ld\n", query.state_flags);
            printf("result not found\n");
            exit(1);
        }
        memcpy(retval, query.value, sizeof(query.value));
    } else {
        /* Traverse b+ tree index in db to find value */
        Node *const node;
        if (posix_memalign((void **) &node, 512, sizeof(Node))) {
            perror("posix_memalign failed");
            exit(1);
        }

        checked_pread(db_fd, node, sizeof(Node), 0);
        ptr__t ptr = nxt_node(key, node);
        while (node->type != LEAF) {
            checked_pread(db_fd, node, sizeof(Node), (long) decode(ptr));
            ptr = nxt_node(key, node);
        }
        /* Verify that the key exists in this leaf node, otherwise missing key */
        if (!key_exists(key, node)) {
            free(node);
            free(retval);
            close(db_fd);
            return NULL;
        }

        /* Now we're at a leaf node, and ptr points to the log entry */
        read_value_the_hard_way(db_fd, retval, ptr);
        free(node);
    }
    close(db_fd);
    return retval;
}

int main(int argc, char *argv[]) {
    if (argc != 3 && argc != 4) {
        fprintf(stderr, "%s file key", *argv);
    }

    char *file_name = argv[1];

    char *endptr = NULL;
    unsigned long const key = strtoul(argv[2], &endptr, 10);
    if (endptr == argv[2] || errno != 0) {
        fprintf(stderr, "Invalid key\n");
        exit(1);
    }

    /* Check if we should use XRP for the read or not */
    int use_xrp = 0;
    char c;
    while ((c = getopt(argc, argv, "x")) != -1) {
        if (c == 'x') {
            use_xrp = 1;
            break;
        }
    }

    char *val = grab_value(file_name, key, use_xrp);
    if (val == NULL) {
        printf("value not found\n");
        return 2;
    }
    char *nospace = val;
    while (*nospace != '\0' && isspace((int) *nospace)) {
        ++nospace;
    }
    printf("got value: %s\n", nospace);
    free(val);
    return 0;
}
