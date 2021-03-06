#include "db.h"
#include <stdlib.h>
#include <string.h>

void initialize(size_t layer_num, int mode) {
    if (mode == LOAD_MODE) {
        db = fopen(DB_PATH, "w+");
    } else {
        db = fopen(DB_PATH, "r+");
    }

    if (db == NULL) {
        printf("Fail to open file %s!\n", DB_PATH);
        exit(0);
    }

    layer_cap = (size_t *)malloc(layer_num * sizeof(size_t));
    total_node = 1;
    layer_cap[0] = 1;
    for (size_t i = 1; i < layer_num; i++) {
        layer_cap[i] = layer_cap[i - 1] * FANOUT;
        total_node += layer_cap[i];
    }
    max_key = layer_cap[layer_num - 1] * NODE_CAPACITY;

    printf("%lu blocks in total, max key is %lu\n", total_node, max_key);
}

int terminate() {
    printf("Done!\n");
    free(layer_cap);
    fclose(db);
    return 0;
}

int compare_nodes(Node *x, Node *y) {
    if (x->num != y->num) {
        printf("num differs %lu %lu\n", x->num, y->num);
        return 0;
    }
    if (x->type != y->type) {
        printf("type differs %lu %lu\n", x->type, y->type);
        return 0;
    }
    for (size_t i = 0; i < x->num; i++)
        if (x->key[i] != y->key[i] || x->ptr[i] != y->ptr[i]) {
            printf("bucket %lu differs x.key %lu y.key %lu x.ptr %lu y.ptr %lu\n",
                    i, x->key[i], y->key[i], x->ptr[i], y->ptr[i]);
            return 0;
        }
    return 1;
}

int load(size_t layer_num) {
    printf("Load the database of %lu layers\n", layer_num);
    initialize(layer_num, LOAD_MODE);

    // 1. Load the index
    Node node, tmp;
    ptr__t next_pos = 1, tmp_ptr = 0;
    for (size_t i = 0; i < layer_num; i++) {
        size_t extent = max_key / layer_cap[i], start_key = 0;
        printf("layer %lu extent %lu\n", i, extent);
        for (size_t j = 0; j < layer_cap[i]; j++) {
            node.num = NODE_CAPACITY;
            node.type = (i == layer_num - 1) ? LEAF : INTERNAL;
            size_t sub_extent = extent / node.num;
            for (size_t k = 0; k < node.num; k++) {
                node.key[k] = start_key + k * sub_extent;
                node.ptr[k] = node.type == INTERNAL ? 
                              next_pos   * BLK_SIZE :
                              total_node * BLK_SIZE + (next_pos - total_node) * VAL_SIZE;
                next_pos++;
            }
            fwrite(&node, sizeof(Node), 1, db);
            start_key += extent;

            // Sanity check
            // read_node(tmp_ptr, &tmp);
            // compare_nodes(&node, &tmp);
            // tmp_ptr += BLK_SIZE;
        }
    }

    // 2. Load the value log
    Log log;
    for (size_t i = 0; i < max_key; i += LOG_CAPACITY) {
        for (size_t j = 0; j < LOG_CAPACITY; j++) {
            sprintf(log.val[j], "%63lu", i + j);
        }
        fwrite(&log, sizeof(Log), 1, db);

        // Sanity check
        // read_log((total_node + i / LOG_CAPACITY) * BLK_SIZE, &log);
    }

    return terminate();
}

int run(size_t layer_num, size_t request_num) {
    printf("Run the test of %lu requests\n", request_num);
    initialize(layer_num, RUN_MODE);

    srand(2021);
    for (size_t i = 0; i < request_num; i++) {
        key__t key = rand() % max_key;
        val__t val;
        get(key, val);
        if (key != atoi(val)) {
            printf("Error! key: %lu val: %s\n", key, val);
        }
    }

    return terminate();
}

int get(key__t key, val__t val) {
    ptr__t ptr = 0; // Start from the root
    Node node;

    do {
        ptr = next_node(key, ptr, &node);
    } while (node.type != LEAF);

    return retrieve_value(ptr, val);
}

void print_node(ptr__t ptr, Node *node) {
    printf("----------------\n");
    printf("ptr %lu num %lu type %lu\n", ptr, node->num, node->type);
    for (size_t i = 0; i < NODE_CAPACITY; i++) {
        printf("(%6lu, %8lu) ", node->key[i], node->ptr[i]);
    }
    printf("\n----------------\n");
}

void print_log(ptr__t ptr, Log *log) {
    printf("----------------\n");
    printf("ptr %lu\n", ptr);
    for (size_t i = 0; i < LOG_CAPACITY; i++) {
        printf("%s\n", log->val[i]);
    }
    printf("\n----------------\n");
}

void read_node(ptr__t ptr, Node *node) {
    fseek(db, ptr, SEEK_SET);
    fread(node, sizeof(Node), 1, db);
    
    // Debug output
    // print_node(ptr, node);
}

void read_log(ptr__t ptr, Log *log) {
    fseek(db, ptr, SEEK_SET);
    fread(log, sizeof(Log), 1, db);

    // Debug output
    // print_log(ptr, log);
}

int retrieve_value(ptr__t ptr, val__t val) {
    Log log;
    ptr__t mask = BLK_SIZE - 1;
    ptr__t base = ptr & (~mask);
    ptr__t offset = ptr & mask;

    read_log(base, &log);
    memcpy(val, log.val[offset / VAL_SIZE], VAL_SIZE);

    return 0;
}

ptr__t next_node(key__t key, ptr__t ptr, Node *node) {
    read_node(ptr, node);
    for (size_t i = 0; i < node->num; i++) {
        if (key < node->key[i]) {
            return node->ptr[i - 1];
        }
    }
    return node->ptr[node->num - 1];
}

int prompt_help() {
    printf("Usage: ./db --load number_of_layers\n");
    printf("or     ./db --run number_of_layers number_of_requests\n");
    return 0;
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        return prompt_help(argc, argv);
    } else if (strcmp(argv[1], "--load") == 0) {
        return load(atoi(argv[2]));
    } else if (strcmp(argv[1], "--run") == 0) {
        if (argc < 4) {
            return prompt_help();
        }
        return run(atoi(argv[2]), atoi(argv[3]));
    } else {
        return prompt_help();
    }

    return 0;
}