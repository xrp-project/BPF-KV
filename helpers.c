#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>

#include "helpers.h"

/**
 * Get the leaf node that MAY contain [key].
 *
 * Note: It is up to the caller to verify that the node actually contains [key].
 * If it does not, then [key] does not exist in the database.
 *
 * @param database_fd - File descriptor for open database file
 * @param key
 * @param *node - Pointer to Node that will be populated on success
 * @return 0 on success (node retrieved), -1 on error
 */
int _get_leaf_containing(int database_fd, key__t key, Node *node, ptr__t index_offset, ptr__t *node_offset) {
    Node *const tmp_node = (Node *) aligned_alloca(BLK_SIZE, sizeof(Node));
    long bytes_read = pread(database_fd, tmp_node, sizeof(Node), (long) index_offset);
    if (bytes_read != sizeof(Node)) {
        return -1;
    }
    ptr__t ptr = nxt_node(key, tmp_node);
    while (tmp_node->type != LEAF) {
        bytes_read = pread(database_fd, tmp_node, sizeof(Node), (long) decode(ptr));
        if (bytes_read != sizeof(Node)) {
            return -1;
        }
        *node_offset = ptr;
        ptr = nxt_node(key, tmp_node);
    }
    *node = *tmp_node;
    return 0;
}

int get_leaf_containing(int database_fd, key__t key, Node *node, ptr__t index_offset) {
    ptr__t x = 0;
    return _get_leaf_containing(database_fd, key, node, index_offset, &x);
}

long lookup_bpf(int db_fd, int bpf_fd, struct Query *query, ptr__t index_offset) {
    /* Set up buffers and query */
    char *buf = (char *) aligned_alloca(0x1000, 0x1000);
    char *scratch = (char *) aligned_alloca(0x1000, SCRATCH_SIZE);
    memset(buf, 0, 0x1000);
    memset(scratch, 0, 0x1000);

    struct ScatterGatherQuery *sgq = (struct ScatterGatherQuery *) scratch;
    sgq->keys[0] = query->key;
    sgq->n_keys = 1;

    /* Syscall to invoke BPF function that we loaded out-of-band previously */
    long ret = syscall(SYS_READ_XRP, db_fd, buf, BLK_SIZE, index_offset, bpf_fd, scratch);

    struct MaybeValue *maybe_v = &sgq->values[0];
    query->found = (long) maybe_v->found;
    if (query->found) {
        memcpy(query->value, maybe_v->value, sizeof(val__t));
    }

    return ret;
}

/* Helper function that terminates the program is pread fails */
void checked_pread(int fd, void *buf, size_t size, long offset) {
    ssize_t bytes_read = pread(fd, buf, size, offset);
    if (bytes_read < 0) {
        perror("checked_pread: ");
        exit(1);
    }
    if ((size_t) bytes_read != size) {
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
    for (size_t i = 1; i < NODE_CAPACITY; ++i) {
        if (key < node->key[i]) {
            return node->ptr[i - 1];
        }
    }
    /* Key wasn't smaller than any of node->key[x], so take the last ptr */
    return node->ptr[NODE_CAPACITY - 1];
}

/**
 * Verifies that [key] exists in the leaf [node]
 * @param key
 * @param node
 * @return 1 if [key] exists, else 0
 */
int key_exists(unsigned long const key, Node const *node) {
    for (unsigned int i = 0; i < NODE_CAPACITY; ++i) {
        if (node->key[i] == key) {
            return 1;
        }
    }
    return 0;
}

int compare_nodes(Node *x, Node *y) {
    if (x->type != y->type) {
        printf("type differs %lu %lu\n", x->type, y->type);
        return 0;
    }
    for (size_t i = 0; i < NODE_CAPACITY; i++)
        if (x->key[i] != y->key[i] || x->ptr[i] != y->ptr[i]) {
            printf("bucket %lu differs x.key %lu y.key %lu x.ptr %lu y.ptr %lu\n",
                    i, x->key[i], y->key[i], x->ptr[i], y->ptr[i]);
            return 0;
        }
    return 1;
}

long calculate_max_key(unsigned int layers) {
    long result = 1;
    for (unsigned int i = 0; i < layers; ++i) {
        result *= FANOUT;
    }
    /* Subtract one due to zero indexing of keys */
    return result - 1;
}

int load_bpf_program(char *path) {
    struct bpf_object *obj;
    int ret, progfd;

    ret = bpf_prog_load(path, BPF_PROG_TYPE_XRP, &obj, &progfd);
    if (ret) {
        printf("Failed to load bpf program\n");
        exit(1);
    }

    return progfd;
}
