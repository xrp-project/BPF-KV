#include "db.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int load(unsigned int num) {
    printf("Load the database of %u layers\n", num);
    return 0;
}

int run(unsigned int num) {
    printf("Run the test of %u requests\n", num);
    return 0;
}

v_type get(k_type k) {
    v_type v = 0;

    return v;
}

Node read_node(p_type p) {
    Node node;

    return node;
}

p_type next_node(k_type k, p_type p) {
    p_type next = 0;

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