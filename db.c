#include "db.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h>

int get_handler(int flag) {
    int fd = open(DB_PATH, flag, 0755);
    if (fd < 0) {
        printf("Fail to open file %s!\n", DB_PATH);
        exit(0);
    }
    return fd;
}

int get_load_handler(int flag) {
    int fd = open(DB_PATH, flag, 0755);
    if (fd < 0) {
        printf("Fail to open file %s!\n", DB_PATH);
        exit(0);
    }
    return fd;
}

void initialize(size_t layer_num, int mode) {
    if (mode == LOAD_MODE) {
        db = get_load_handler(O_CREAT|O_TRUNC|O_WRONLY);
    } else {
        db = get_handler(O_RDONLY|O_DIRECT);
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
    
    if (posix_memalign((void **)&cache, 512, entry_num * sizeof(Node))) {
        perror("posix_memalign failed");
        exit(1);
    }

    size_t head = 0, tail = 1;
    read_node(encode(0), &cache[head], db);

    while (tail < entry_num) {
        for (size_t i = 0; i < cache[head].num; i++) {
            read_node(cache[head].ptr[i], &cache[tail], db);
            cache[head].ptr[i] = (ptr__t)(&cache[tail]); // in-memory cache entry has in-memory pointer
            tail++;
        }
        head++;
    }

    cache_cap = entry_num; // enable the cache
    printf("Cache built. %lu layers %lu entries in total.\n", layer_num, entry_num);
}

int terminate() {
    printf("Done!\n");
    free(layer_cap);
    free(cache);
    close(db);
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
    Node *node, *tmp;
    if (posix_memalign((void **)&node, 512, sizeof(Node))) {
        perror("posix_memalign failed");
        exit(1);
    }
    ptr__t next_pos = 1, tmp_ptr = 0;
    for (size_t i = 0; i < layer_num; i++) {
        size_t extent = max_key / layer_cap[i], start_key = 0;
        printf("layer %lu extent %lu\n", i, extent);
        for (size_t j = 0; j < layer_cap[i]; j++) {
            node->num = NODE_CAPACITY;
            node->type = (i == layer_num - 1) ? LEAF : INTERNAL;
            size_t sub_extent = extent / node->num;
            for (size_t k = 0; k < node->num; k++) {
                node->key[k] = start_key + k * sub_extent;
                node->ptr[k] = node->type == INTERNAL ? 
                              encode(next_pos   * BLK_SIZE) :
                              encode(total_node * BLK_SIZE + (next_pos - total_node) * VAL_SIZE);
                next_pos++;
            }
            write(db, node, sizeof(Node));
            start_key += extent;

            // Sanity check
            // read_node(tmp_ptr, &tmp);
            // compare_nodes(&node, &tmp);
            // tmp_ptr += BLK_SIZE;
        }
    }

    // 2. Load the value log
    Log *log;
    if (posix_memalign((void **)&log, 512, sizeof(Log))) {
        perror("posix_memalign failed");
        exit(1);
    }
    for (size_t i = 0; i < max_key; i += LOG_CAPACITY) {
        for (size_t j = 0; j < LOG_CAPACITY; j++) {
            sprintf(log->val[j], "%63lu", i + j);
        }
        write(db, log, sizeof(Log));

        // Sanity check
        // read_log((total_node + i / LOG_CAPACITY) * BLK_SIZE, &log);
    }

    free(log);
    free(node);
    return terminate();
}

void initialize_workers(WorkerArg *args, size_t total_op_count) {
    for (size_t i = 0; i < worker_num; i++) {
        args[i].index = i;
        args[i].op_count = (total_op_count / worker_num) + (i < total_op_count % worker_num);
        args[i].db_handler = get_handler(O_RDONLY|O_DIRECT);
        args[i].timer = 0;
    }
}

void start_workers(pthread_t *tids, WorkerArg *args) {
    for (size_t i = 0; i < worker_num; i++) {
        pthread_create(&tids[i], NULL, subtask, (void*)&args[i]);
    }
}

void terminate_workers(pthread_t *tids, WorkerArg *args) {
    for (size_t i = 0; i < worker_num; i++) {
        pthread_join(tids[i], NULL);
        close(args[i].db_handler);
    }
}

int run(size_t layer_num, size_t request_num, size_t thread_num) {
    printf("Run the test of %lu requests\n", request_num);
    initialize(layer_num, RUN_MODE);
    build_cache(layer_num > 2 ? 2 : layer_num);

    worker_num = thread_num;
    struct timeval start, end;
    pthread_t tids[worker_num];
    WorkerArg args[worker_num];

    initialize_workers(args, request_num);

    gettimeofday(&start, NULL);
    start_workers(tids, args);
    terminate_workers(tids, args);
    gettimeofday(&end, NULL);

    long total_latency = 0;
    for (size_t i = 0; i < worker_num; i++) total_latency += args[i].timer;
    long run_time = 1000000 * (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec);

    printf("Average throughput: %f op/s latency: %f usec\n", 
            (double)request_num / run_time * 1000000, (double)total_latency / request_num);

    return terminate();
}

void *subtask(void *args) {
    WorkerArg *r = (WorkerArg*)args;
    struct timeval start, end;
    struct timespec tps, tpe;
    srand(r->index);
    printf("thread %ld op_count %ld\n", r->index, r->op_count);
    for (size_t i = 0; i < r->op_count; i++) {
        key__t key = rand() % max_key;
        val__t val;

        gettimeofday(&start, NULL);
	clock_gettime(CLOCK_REALTIME, &tps);
        get(key, val, r->db_handler);
	clock_gettime(CLOCK_REALTIME, &tpe);
        gettimeofday(&end, NULL);
        unsigned long btt = (tpe.tv_nsec - tps.tv_nsec);
        r->timer += 1000000 * (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec);
	printf("%lu\n", btt);
        if (key != atoi(val)) {
            printf("Error! key: %lu val: %s thrd: %ld\n", key, val, r->index);
        }       
    }
}

int get(key__t key, val__t val, int db_handler) {
    //ptr__t ptr = cache_cap > 0 ? (ptr__t)(&cache[0]) : encode(0);
    ptr__t ptr = encode(0);
    Node *node;

    if (posix_memalign((void **)&node, 512, sizeof(Node))) {
        perror("posix_memalign failed");
        exit(1);
    }
    /*
    if (cache_cap > 0) {
        do {
            ptr = next_node(key, (Node *)ptr);
        } while (!is_file_offset(ptr));
    }
    */

    do {
	//printf("PTR: %lu\n", decode(ptr));
        read_node(ptr, node, db_handler);
	//print_node(ptr, node);
        ptr = next_node(key, node);
    } while (node->type != LEAF);

    //printf("FINAL PTR:%d\n", ptr);

    return retrieve_value(ptr, val, db_handler);
}

void print_node(ptr__t ptr, Node *node) {
    printf("----------------\n");
    printf("ptr %lu num %lu type %lu\n", decode(ptr), node->num, node->type);
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

void read_node(ptr__t ptr, Node *node, int db_handler) {
    //lseek(db_handler, decode(ptr), SEEK_SET);
    pread(db_handler, node, sizeof(Node), decode(ptr));
    
    // Debug output
    // print_node(ptr, node);
}

void read_log(ptr__t ptr, Log *log, int db_handler) {
    //lseek(db_handler, decode(ptr), SEEK_SET);
    pread(db_handler, log, sizeof(Log), decode(ptr));

    // Debug output
    // print_log(ptr, log);
}

int retrieve_value(ptr__t ptr, val__t val, int db_handler) {
    Log *log;
    if (posix_memalign((void **)&log, 512, sizeof(Log))) {
        perror("posix_memalign failed");
        exit(1);
    }

    ptr__t mask = BLK_SIZE - 1;
    ptr__t base = ptr & (~mask);
    ptr__t offset = ptr & mask;

    read_log(base, log, db_handler);
    memcpy(val, log->val[offset / VAL_SIZE], VAL_SIZE);

    return 0;
}

ptr__t next_node(key__t key, Node *node) {
    for (size_t i = 0; i < node->num; i++) {
        if (key < node->key[i]) {
            return node->ptr[i - 1];
        }
    }
    return node->ptr[node->num - 1];
}

int prompt_help() {
    printf("Usage: ./db --load number_of_layers\n");
    printf("or     ./db --run number_of_layers number_of_requests number_of_threads\n");
    return 0;
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        return prompt_help(argc, argv);
    } else if (strcmp(argv[1], "--load") == 0) {
        return load(atoi(argv[2]));
    } else if (strcmp(argv[1], "--run") == 0) {
        if (argc < 5) {
            return prompt_help();
        }
        return run(atoi(argv[2]), atoi(argv[3]), atoi(argv[4]));
    } else {
        return prompt_help();
    }

    return 0;
}
