#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>

#include "get.h"
#include "parse.h"
#include "db_types.h"
#include "helpers.h"
#include "simplekv.h"


int do_get_cmd(int argc, char *argv[], struct ArgState *as) {
    struct GetArgs ga = {
            .database_layers = as->layers,
            .threads = 1,
            .requests = 500
    };
    parse_get_opts(argc, argv, &ga);

    /* Load BPF program */
    int bpf_fd = -1;
    if (ga.xrp) {
        bpf_fd = load_bpf_program("xrp-bpf/get.o");
    }

    if (ga.key_set) {
        return lookup_single_key(as->filename, ga.key, ga.xrp, bpf_fd);
    }

    return run(as->filename, as->layers, ga.requests, ga.threads, ga.xrp, bpf_fd, ga.cache_level);
}



int lookup_single_key(char *filename, long key, int use_xrp, int bpf_fd) {
    /* Lookup Single Key */
    char *value = grab_value(filename, key, use_xrp, bpf_fd, ROOT_NODE_OFFSET);
    printf("Key: %ld\n", key);
    if (value == NULL) {
        printf("Value not found\n");
        return 1;
    }
    char *nospace = value;
    while (*nospace != '\0' && isspace((int) *nospace)) {
        ++nospace;
    }
    printf("Value %s\n", nospace);
    free(value);
    return 0;
}

char *grab_value(char *file_name, unsigned long const key, int use_xrp, int bpf_fd, ptr__t index_offset) {
    char *const retval = malloc(sizeof(val__t) + 1);
    if (retval == NULL) {
        perror("malloc");
        exit(1);
    }
    /* Ensure we have a null at the end of the string */
    retval[sizeof(val__t)] = '\0';

    /* Open the database */
    int flags = O_RDONLY;
    if (use_xrp) {
        flags = flags | O_DIRECT;
    }
    int db_fd = open(file_name, flags);
    if (db_fd < 0) {
        perror("failed to open database");
        exit(1);
    }

    struct Query query = new_query(key);
    if (use_xrp) {
        long ret = lookup_bpf(db_fd, bpf_fd, &query, index_offset);

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
    } else {
        if (lookup_key_userspace(db_fd, &query, index_offset)) {
            free(retval);
            close(db_fd);
            return NULL;
        }
        /* Traverse b+ tree index in db to find value and verify the key exists in leaf node */
    }
    memcpy(retval, query.value, sizeof(query.value));
    close(db_fd);
    return retval;
}

/**
 * Traverses the B+ tree index and retrieves the value associated with
 * [key] from the heap, if [key] is in the database.
 * @param file_name
 * @param key
 * @param index_offset Offset into the B+tree index to begin the traversal inside the index
 *        if caching is used.
 * @return null terminated string containing the value on disk, or NULL if key not found
 */

long lookup_key_userspace(int db_fd, struct Query *query, ptr__t index_offset) {
    /* Traverse b+ tree index in db to find value and verify the key exists in leaf node */
    Node node = { 0 };
    if (get_leaf_containing(db_fd, query->key, &node, index_offset) != 0 || !key_exists(query->key, &node)) {
        query->found = 0;
        return -1;
    }
    read_value_the_hard_way(db_fd, (char *) query->value, nxt_node(query->key, &node));
    query->found = 1;
    return 0;
}

/* Function using the same bit fiddling that we use in the BPF function */
void read_value_the_hard_way(int fd, char *retval, ptr__t ptr) {
    /* Aligned buffer for O_DIRECT read */
    char *buf = (char *) aligned_alloca(BLK_SIZE, BLK_SIZE);

    /* Base of the block containing our value */
    ptr__t base = decode(ptr) & ~(BLK_SIZE - 1);
    checked_pread(fd, buf, BLK_SIZE, (long) base);
    ptr__t offset = decode(ptr) & (BLK_SIZE - 1);
    memcpy(retval, buf + offset, sizeof(val__t));
}
