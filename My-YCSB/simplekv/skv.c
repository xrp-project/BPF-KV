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

void initialize(size_t layer_num, int mode) {
    if (mode == LOAD_MODE) {
        db = get_handler(O_CREAT|O_TRUNC|O_WRONLY);
    } else {
        db = get_handler(O_RDWR|O_DIRECT);
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
    size_t log_num = max_key / LOG_CAPACITY + 1;
    val_lock = (pthread_mutex_t *)malloc(log_num * sizeof(pthread_mutex_t));
    if (val_lock == NULL) {
        printf("val_lock malloc fail!\n");
        terminate();
        exit(1);
    }
    for (size_t i = 0; i < log_num; i++) {
        if (pthread_mutex_init(&val_lock[i], NULL) != 0) {
            printf("pthread_mutex_init fail at %lu\n", i);
            terminate();
            exit(1);
        }
    }
    printf("%lu locks\n", log_num);

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
    if (val_lock != NULL) {
        free(val_lock);
    }
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
        args[i].db_handler = get_handler(O_RDWR|O_DIRECT);
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
    build_cache(layer_num > 3 ? 3 : layer_num);

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

    srand(r->index);
    printf("thread %ld op_count %ld\n", r->index, r->op_count);
    for (size_t i = 0; i < r->op_count; i++) {
        key__t key = rand() % max_key;
        val__t val;

        int type = rand() % 100;
        if (type < read_ratio) {
            gettimeofday(&start, NULL);
            get(key, val, r->db_handler);
            gettimeofday(&end, NULL);
        } else if (type < read_ratio + rmw_ratio) {
            gettimeofday(&start, NULL);    
            read_modify_write(key, val, r->db_handler);
            gettimeofday(&end, NULL);
        } else {
            sprintf(val, "%63d", rand());

            gettimeofday(&start, NULL);    
            update(key, val, r->db_handler);
            gettimeofday(&end, NULL);
        }

        r->timer += 1000000 * (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec); 
    }
}

int get(key__t key, val__t val, int db_handler) {
    ptr__t ptr = cache_cap > 0 ? (ptr__t)(&cache[0]) : encode(0);
    Node *node;

    if (posix_memalign((void **)&node, 512, sizeof(Node))) {
        perror("posix_memalign failed");
        exit(1);
    }

    if (cache_cap > 0) {
        do {
            ptr = next_node(key, (Node *)ptr);
        } while (!is_file_offset(ptr));
    }

    do {
        read_node(ptr, node, db_handler);
        ptr = next_node(key, node);
    } while (node->type != LEAF);

    return retrieve_value(ptr, val, db_handler);
}

void update(key__t key, val__t val, int db_handler) {
    ptr__t ptr = cache_cap > 0 ? (ptr__t)(&cache[0]) : encode(0);
    Node *node;

    if (posix_memalign((void **)&node, 512, sizeof(Node))) {
        perror("posix_memalign failed");
        exit(1);
    }

    if (cache_cap > 0) {
        do {
            ptr = next_node(key, (Node *)ptr);
        } while (!is_file_offset(ptr));
    }

    do {
        read_node(ptr, node, db_handler);
        ptr = next_node(key, node);
    } while (node->type != LEAF);

    
    update_value(ptr, val, db_handler);
}

void read_modify_write(key__t key, val__t val, int db_handler) {
    ptr__t ptr = cache_cap > 0 ? (ptr__t)(&cache[0]) : encode(0);
    Node *node;

    if (posix_memalign((void **)&node, 512, sizeof(Node))) {
        perror("posix_memalign failed");
        exit(1);
    }

    if (cache_cap > 0) {
        do {
            ptr = next_node(key, (Node *)ptr);
        } while (!is_file_offset(ptr));
    }

    do {
        read_node(ptr, node, db_handler);
        ptr = next_node(key, node);
    } while (node->type != LEAF);

    
    read_modify_write_value(ptr, val, db_handler);
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

void read_node(ptr__t ptr, Node *node, int db_handler) {
    lseek(db_handler, decode(ptr), SEEK_SET);
    read(db_handler, node, sizeof(Node));
    
    // Debug output
    // print_node(ptr, node);
}

void read_log(ptr__t ptr, Log *log, int db_handler) {
    lseek(db_handler, decode(ptr), SEEK_SET);
    read(db_handler, log, sizeof(Log));

    // Debug output
    // print_log(ptr, log);
}

void write_log(ptr__t ptr, Log *log, int db_handler) {
    lseek(db_handler, decode(ptr), SEEK_SET);
    write(db_handler, log, sizeof(Log));

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

void update_value(ptr__t ptr, val__t val, int db_handler) {
    Log *log;
    if (posix_memalign((void **)&log, 512, sizeof(Log))) {
        perror("posix_memalign failed");
        exit(1);
    }

    ptr__t mask = BLK_SIZE - 1;
    ptr__t base = ptr & (~mask);
    ptr__t offset = ptr & mask;
    size_t log_idx = decode(base) / BLK_SIZE - total_node;

    read_log(base, log, db_handler);
    memcpy(log->val[offset / VAL_SIZE], val, VAL_SIZE);

    int rc = 0;
    if ((rc = pthread_mutex_lock(&val_lock[log_idx])) != 0) {
        printf("lock error %d %lu\n", rc, log_idx);
    }

    write_log(base, log, db_handler);

    pthread_mutex_unlock(&val_lock[log_idx]);
}

void read_modify_write_value(ptr__t ptr, val__t val, int db_handler) {
    Log *log;
    if (posix_memalign((void **)&log, 512, sizeof(Log))) {
        perror("posix_memalign failed");
        exit(1);
    }

    ptr__t mask = BLK_SIZE - 1;
    ptr__t base = ptr & (~mask);
    ptr__t offset = ptr & mask;
    size_t log_idx = decode(base) / BLK_SIZE - total_node;

    int rc = 0;
    if ((rc = pthread_mutex_lock(&val_lock[log_idx])) != 0) {
        printf("lock error %d %lu\n", rc, log_idx);
    }

    read_log(base, log, db_handler);
    size_t num = atoi(log->val[offset / VAL_SIZE]);
    sprintf(log->val[offset / VAL_SIZE], "%63lu", num / 2);
    write_log(base, log, db_handler);

    pthread_mutex_unlock(&val_lock[log_idx]);
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
    printf("or     ./db --run number_of_layers number_of_requests number_of_threads read_ratio rmw_ratio\n");
    return 0;
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        return prompt_help(argc, argv);
    } else if (strcmp(argv[1], "--load") == 0) {
        return load(atoi(argv[2]));
    } else if (strcmp(argv[1], "--run") == 0) {
        if (argc < 7) {
            return prompt_help();
        }
        read_ratio = atoi(argv[5]);
        rmw_ratio = atoi(argv[6]);
        return run(atoi(argv[2]), atoi(argv[3]), atoi(argv[4]));
    } else {
        return prompt_help();
    }

    return 0;
}