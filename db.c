#include "db.h"
#include <stdlib.h>
#include <string.h>

void initialize(size_t layer_num) {
    db = fopen(DB_PATH, "w+");

    layer_cap = (size_t *)malloc(layer_num * sizeof(size_t));
    total_node = 1;
    layer_cap[0] = 1;
    for (size_t i = 1; i < layer_num; i++) {
        layer_cap[i] = layer_cap[i - 1] * FANOUT;
        total_node += layer_cap[i];
    }
    printf("%lu blocks in total\n", total_node);
}

int load(size_t layer_num) {
    printf("Load the database of %lu layers\n", layer_num);
    initialize(layer_num);

    Node node;
    ptr__t next_pos = 1;
    for (size_t i = 0; i < layer_num; i++) {
        size_t extent = total_node / layer_cap[i], start_key = 0;
        for (size_t j = 0; j < layer_cap[i]; j++) {
            node.num = NODE_CAPACITY;
            node.type = (j == layer_num - 1) ? LEAF : INTERNAL;
            size_t sub_extent = extent / node.num;
            for (size_t k = 0; k < node.num; k++) {
                node.key[k] = start_key + k * sub_extent;
                node.ptr[k] = next_pos * BLK_SIZE;
                next_pos++;
            }
            fwrite(&node, sizeof(Node), 1, db);
            start_key += extent;
        }
    }

    free(layer_cap);
    fclose(db);
    return 0;
}

int run(size_t layer_num, size_t request_num) {
    printf("Run the test of %lu requests\n", request_num);
    initialize(layer_num);

    size_t max_key = layer_cap[layer_num] * NODE_CAPACITY;
    srand(2021);
    for (size_t i = 0; i < request_num; i++) {
        key__t key = rand() % max_key;
        val__t val = get(key);
    }

    fclose(db);
    return 0;
}

val__t get(key__t key) {
    ptr__t ptr = 0; // Start from the root
    Node node;

    do {
        ptr = next_node(key, ptr, &node);
    } while (node.type != LEAF);

    return search_value(ptr, key);
}

void read_node(ptr__t ptr, Node *node) {
    fseek(db, ptr, SEEK_SET);
    fread(node, sizeof(Node), 1, db);
}

val__t search_value(ptr__t ptr, key__t key) {
    val__t val = 0;
    
    return val;
}


ptr__t next_node(key__t key, ptr__t ptr, Node *node) {
    read_node(ptr, node);
    for (size_t i = 0; i < node->num; i++) {
        if (key < node->key[i]) {
            return node->ptr[i - 1];
        }
    }
    return 0;
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