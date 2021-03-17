#include "db.h"
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

FILE* get_handler(char *flag) {
    FILE *handler = fopen(DB_PATH, flag);
    if (handler == NULL) {
        printf("Fail to open file %s!\n", DB_PATH);
        exit(0);
    }
    return handler;
}

void initialize(size_t layer_num, int mode) {
    if (mode == LOAD_MODE) {
        db = get_handler("w+");
    } else {
        db = get_handler("r+");
    }
    
    layer_cap = (size_t *)malloc(layer_num * sizeof(size_t));
    total_node = 1;
    layer_cap[0] = 1;
    for (size_t i = 1; i < layer_num; i++) {
        layer_cap[i] = layer_cap[i - 1] * FANOUT;
        total_node += layer_cap[i];
    }
    max_key = layer_cap[layer_num - 1] * NODE_CAPACITY;
    cache_cap = 0;

    printf("%lu blocks in total, max key is %lu\n", total_node, max_key);
}

void build_cache(size_t layer_num) {
    size_t entry_num = 0;
    for (size_t i = 0; i < layer_num; i++) {
        entry_num += layer_cap[i];
    }
    
    cache = (CacheEntry *)malloc(entry_num * sizeof(CacheEntry));

    size_t head = 0, tail = 1;
    cache[head].ptr = 0; // start from the root
    read_node(cache[head].ptr, &cache[head].node, db);

    while (tail < entry_num) {
        for (size_t i = 0; i < cache[head].node.num; i++) {
            cache[tail].ptr = cache[head].node.ptr[i];
            read_node(cache[tail].ptr, &cache[tail].node, db);
            tail++;
        } 
        head++;
    }

    cache_cap = entry_num; // enable the cache
    printf("Cache built. %lu layers %lu entries in total.\n", layer_num, entry_num);
}

int is_cached(ptr__t ptr, Node *node) {
    for (size_t i = 0; i < cache_cap; i++) {
        if (cache[i].ptr == ptr) {
            *node = cache[i].node;
            return 1;
        }
    }
    return 0;
}

int terminate() {
    printf("Done!\n");
    free(layer_cap);
    free(cache);
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

void initialize_workers(WorkerArg *args, size_t op_count_per_worker) {
    for (size_t i = 0; i < WORKER_NUM; i++) {
        args[i].index = i;
        args[i].op_count = op_count_per_worker;
        args[i].db_handler = get_handler("r+");
    }
}

void start_workers(pthread_t *tids, WorkerArg *args) {
    for (size_t i = 0; i < WORKER_NUM; i++) {
        pthread_create(&tids[i], NULL, subtask, (void*)&args[i]);
    }
}

void terminate_workers(pthread_t *tids, WorkerArg *args) {
    for (size_t i = 0; i < WORKER_NUM; i++) {
        pthread_join(tids[i], NULL);
        fclose(args[i].db_handler);
    }
}

int run(size_t layer_num, size_t request_num) {
    printf("Run the test of %lu requests\n", request_num);
    initialize(layer_num, RUN_MODE);
    build_cache(layer_num > 3 ? 3 : layer_num);

    struct timeval start, end;
    pthread_t tids[WORKER_NUM];
    WorkerArg args[WORKER_NUM];

    initialize_workers(args, request_num / WORKER_NUM);

    gettimeofday(&start, NULL);
    start_workers(tids, args);
    terminate_workers(tids, args);
    gettimeofday(&end, NULL);

    long run_time = 1000000 * (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec);
    printf("Average throughput: %f op/s\n", 
            (double)request_num / run_time * 1000000);

    return terminate();
}

void *subtask(void *args) {
    WorkerArg *r = (WorkerArg*)args;

    srand(r->index);
    for (size_t i = 0; i < r->op_count; i++) {
        key__t key = rand() % max_key;
        val__t val;
        get(key, val, r->db_handler);
        if (key != atoi(val)) {
            printf("Error! key: %lu val: %s thrd: %ld\n", key, val, r->index);
        }       
    }
}

int get(key__t key, val__t val, FILE *db_handler) {
    ptr__t ptr = 0; // Start from the root
    Node node;

    do {
        ptr = next_node(key, ptr, &node, db_handler);
    } while (node.type != LEAF);

    return retrieve_value(ptr, val, db_handler);
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

void read_node(ptr__t ptr, Node *node, FILE *db_handler) {
    if (!is_cached(ptr, node)) {
        fseek(db_handler, ptr, SEEK_SET);
        fread(node, sizeof(Node), 1, db_handler);
    }
    // Debug output
    // print_node(ptr, node);
}

void read_log(ptr__t ptr, Log *log, FILE *db_handler) {
    fseek(db_handler, ptr, SEEK_SET);
    fread(log, sizeof(Log), 1, db_handler);

    // Debug output
    // print_log(ptr, log);
}

int retrieve_value(ptr__t ptr, val__t val, FILE *db_handler) {
    Log log;
    ptr__t mask = BLK_SIZE - 1;
    ptr__t base = ptr & (~mask);
    ptr__t offset = ptr & mask;

    read_log(base, &log, db_handler);
    memcpy(val, log.val[offset / VAL_SIZE], VAL_SIZE);

    return 0;
}

ptr__t next_node(key__t key, ptr__t ptr, Node *node, FILE *db_handler) {
    read_node(ptr, node, db_handler);
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