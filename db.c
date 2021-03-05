#include "db.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void logistic(size_t layer_num) {
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
    logistic(layer_num);

    printf("Load the database of %lu layers\n", layer_num);
    FILE *idx = fopen(IDX_PATH, "w+");    
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
            fwrite(&node, sizeof(node), 1, idx);
            start_key += extent;
        }
    }

    free(layer_cap);
    fclose(idx);
    return 0;
}

int run(size_t request_num) {
    printf("Run the test of %lu requests\n", request_num);
    return 0;
}

val__t get(key__t k) {
    val__t v = 0;

    return v;
}

Node read_node(ptr__t p) {
    Node node;

    return node;
}

ptr__t next_node(key__t k, ptr__t p) {
    ptr__t next = 0;

    return next;    
}

int prompt_help() {
    printf("Usage: ./db --load number_of_layers\n");
    printf("or     ./db --run number_of_requests\n");
    return 0;
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        return prompt_help(argc, argv);
    } else {
        if (strcmp(argv[1], "--load") == 0) {
            return load(atoi(argv[2]));
        }
        
        if (strcmp(argv[1], "--run") == 0) {
            return run(atoi(argv[2]));
        }

        return prompt_help();
    }
    return 0;
}