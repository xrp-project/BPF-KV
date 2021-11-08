/* This is a simple program that traverses the custom B+ tree structure
 * used by simple-kv.
 *
 * Author: etm2131@columbia.edu
 *
 * Usage:
 * ./simple_retrieve database_file key
 *
 *
 * TODO: Add a flag to use XRP to traverse the tree
 */

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/file.h>
#include <errno.h>
#include <ctype.h>

#include "db.h"

/* Helper function that terminates the program is pread fails */
void checked_pread(int fd, void *buf, size_t size, long offset) {
    ssize_t bytes_read = pread(fd, buf, size, offset);
    if (bytes_read != size) {
        fprintf(stderr, "partial read %ld bytes of Node\n", bytes_read);
        exit(1);
    }
}

/**
 * Finds the file offset for the next node to traverse to in the B+ tree
 * @param key
 * @param node
 * @return B+ tree encoded byte offset into the db file
 */
ptr__t nxt_node(unsigned long key, Node *node) {
    for (size_t i = 1; i < node->num; ++i) {
        if (key < node->key[i]) {
            return node->ptr[i - 1];
        }
    }
    /* Key wasn't smaller than any of node->key[x], so take the last ptr */
    return node->ptr[node->num - 1];
}

/**
 * Verifies that [key] exists in the leaf [node]
 * @param key
 * @param node
 * @return 1 if [key] exists, else 0
 */
int key_exists(unsigned long const key, Node const *node) {
    for (int i = 0; i < node->num; ++i) {
        if (node->key[i] == key) {
            return 1;
        }
    }
    return 0;
}

/**
 * Traverses the B+ tree index and retrieves the value associated with
 * [key] from the heap, if [key] is in the database.
 * @param file_name
 * @param key
 * @return null terminated string containing the value on disk, or NULL if key not found
 */
char *grab_value(char *file_name, unsigned long const key) {
    char *const retval = malloc(sizeof(val__t) + 1);
    if (retval == NULL) {
        perror("malloc");
        exit(1);
    }
    /* Ensure we have a null at the end of the string */
    retval[sizeof(val__t)] = '\0';

    /* Open the database */
    int db_fd = open(file_name, O_RDONLY);
    if (db_fd < 0) {
        perror("failed to open database");
        exit(1);
    }

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
        return NULL;
    }

    /* Now we're at a leaf node, and ptr points to the log entry */
    checked_pread(db_fd, retval, sizeof(val__t), (long) decode(ptr));
    free(node);

    return retval;
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "%s file key", *argv);
    }

    char *file_name = argv[1];

    char *endptr = NULL;
    unsigned long const key = strtoul(argv[2], &endptr, 10);
    if (endptr == argv[2] || errno != 0) {
        fprintf(stderr, "Invalid key\n");
        exit(1);
    }

    char *val = grab_value(file_name, key);
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