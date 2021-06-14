#include "db.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h>

int get_handler(int flag) {
    int fd = open(DB_PATH, flag, 0644);
    if (fd < 0) {
        printf("Fail to open file %s!\n", DB_PATH);
        exit(0);
    }
    return fd;
}

void initialize(size_t layer_num, int mode) {
    if (mode == LOAD_MODE) {
        db = get_handler(O_CREAT|O_TRUNC|O_RDWR);
    } else {
        db = get_handler(O_RDWR|O_DIRECT);
    }
    io_uring_queue_init(QUEUE_DEPTH, &global_ring, 0);
    
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
    if (layer_num == 0) return 0;
    
    size_t entry_num = 0;
    for (size_t i = 0; i < layer_num; i++) {
        entry_num += layer_cap[i];
    }
    
    if (posix_memalign((void **)&cache, 512, entry_num * sizeof(Node))) {
        perror("posix_memalign failed");
        exit(1);
    }

    size_t head = 0, tail = 1;
    read_node(encode(0), &cache[head], db, &global_ring);
    read_complete(&global_ring, 1);
    while (tail < entry_num) {
        for (size_t i = 0; i < cache[head].num; i++) {
            read_node(cache[head].ptr[i], &cache[tail], db, &global_ring);
            read_complete(&global_ring, 1);
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
    io_uring_queue_exit(&global_ring);
    if (val_lock != NULL) {
        free(val_lock);
    }
    return 0;
}

int load(size_t layer_num) {
    printf("Load the database of %lu layers\n", layer_num);
    initialize(layer_num, LOAD_MODE);

    // 1. Load the index
    ptr__t next_pos = 1, tmp_ptr = 0, idx = 0;
    for (size_t i = 0; i < layer_num; i++) {
        size_t extent = max_key / layer_cap[i], start_key = 0;
        printf("layer %lu extent %lu\n", i, extent);
        for (size_t j = 0; j < layer_cap[i]; j++) {
            Node *node, *tmp;
            if (posix_memalign((void **)&node, 512, sizeof(Node))) {
                perror("posix_memalign failed");
                exit(1);
            }
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
            write_node(encode(idx * BLK_SIZE), node, db, &global_ring);
            write_complete(&global_ring);
            start_key += extent;
            idx++;

            // Sanity check
            // if (posix_memalign((void **)&tmp, 512, sizeof(Node))) {
            //     perror("posix_memalign failed");
            //     exit(1);
            // }
            // read_node(encode(idx-1), tmp, db, &global_ring);
            // read_complete(&global_ring, 1);
            // if (memcmp(node, tmp, sizeof(Node)) != 0) {
            //     printf("Wrong node!\n");
            // }
            // free(node);
            // free(tmp);
        }
    }

    // 2. Load the value log
    for (size_t i = 0; i < max_key; i += LOG_CAPACITY) {
        Log *log, *tmp;
        if (posix_memalign((void **)&log, 512, sizeof(Log))) {
            perror("posix_memalign failed");
            exit(1);
        }
        for (size_t j = 0; j < LOG_CAPACITY; j++) {
            sprintf(log->val[j], "%63lu", i + j);
        }
        write_log(encode(idx * BLK_SIZE), log, db, &global_ring);
        write_complete(&global_ring);
        idx++;

        // Sanity check
        // if (posix_memalign((void **)&tmp, 512, sizeof(Log))) {
        //     perror("posix_memalign failed");
        //     exit(1);
        // }
        // read_log(encode(idx-1), tmp, db, &global_ring);
        // read_complete(&global_ring, 0);
        // if (memcmp(log, tmp, sizeof(Log)) != 0) {
        //     printf("Wrong log!\n");
        // }
        // free(log);
        // free(tmp);
    }

    return terminate();
}

void initialize_workers(WorkerArg *args, size_t total_op_count) {
    for (size_t i = 0; i < worker_num; i++) {
        args[i].index = i;
        args[i].op_count = (total_op_count / worker_num) + (i < total_op_count % worker_num);
        args[i].db_handler = get_handler(O_RDWR|O_DIRECT);
        args[i].timer = 0;
        args[i].counter = 0;
        io_uring_queue_init(QUEUE_DEPTH, &args[i].local_ring, 0);
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
    // build_cache(layer_num > cache_layer ? cache_layer : layer_num);

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
            if (i - r->counter > QUEUE_DEPTH) {
                wait_for_completion(&(r->local_ring), &(r->counter), i-1);
            }
            get(key, val, r);
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

    }
    wait_for_completion(&(r->local_ring), &(r->counter), r->op_count);
}

int get(key__t key, val__t val, WorkerArg *r) {
    ptr__t ptr = cache_cap > 0 ? (ptr__t)(&cache[0]) : encode(0);
    Request *req = init_request(key, r);

    // printf("key %ld\n", key);
    // print_node(0, (Node *)ptr);

    if (cache_cap > 0) {
        do {
            ptr = next_node(key, (Node *)ptr);
        } while (!is_file_offset(ptr));

        if (cache_layer == layer_cnt) {
            req->is_value = true;
            ptr__t mask = BLK_SIZE - 1;
            req->ofs = ptr & mask;
            ptr &= (~mask);
        }
    }

    traverse(ptr, req);

    return 0;
    // do {
    //     read_node(ptr, node, r->db_handler, &(r->local_ring));
    //     read_complete(&(r->local_ring), 1);
    //     ptr = next_node(key, node);
    // } while (node->type != LEAF);

    // return retrieve_value(ptr, val, r);
}

void traverse(ptr__t ptr, Request *req) {
    struct io_uring *ring = &(req->warg->local_ring);
    struct iovec *vec = &(req->vec);
    int fd = req->warg->db_handler;

    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    io_uring_prep_readv(sqe, fd, vec, 1, decode(ptr));
    io_uring_sqe_set_data(sqe, req);
    io_uring_submit(ring);
}

void traverse_complete(struct io_uring *ring) {
    struct io_uring_cqe *cqe;
    int ret = io_uring_wait_cqe(ring, &cqe);
    if (ret < 0) {
        perror("io_uring_wait_cqe");
        return;
    }
    if (cqe->res < 0) {
        fprintf(stderr, "[read_complete] Error in async operation: %s\n", strerror(-cqe->res));
        return;
    }

    Request *req = io_uring_cqe_get_data(cqe);
    io_uring_cqe_seen(ring, cqe);

    if (req->is_value) {
        val__t val;
        Log *log = (Log *)req->vec.iov_base;
        memcpy(val, log->val[req->ofs / VAL_SIZE], VAL_SIZE);
        if (req->key != atoi(val)) {
            printf("Errror! key: %lu val: %s\n", req->key, val);
        }            

        struct timeval end;
        gettimeofday(&end, NULL);
        size_t latency = 1000000 * (end.tv_sec - req->start.tv_sec) + (end.tv_usec - req->start.tv_usec);

        // __atomic_fetch_add(req->counter, 1, __ATOMIC_SEQ_CST);
        // __atomic_fetch_add(req->timer, latency, __ATOMIC_SEQ_CST);
        req->warg->counter++;
        req->warg->timer += latency;
        // printf("thread %lu start %ld offset %ld end %ld latency %ld us\n", 
        //         req->thread,
        //         1000000 * req->start.tv_sec + req->start.tv_usec,
        //         1000000 * (req->start.tv_sec - start_tv.tv_sec) + (req->start.tv_usec - start_tv.tv_usec),
        //         1000000 * end.tv_sec + end.tv_usec,
        //         latency);

        free(log);
        free(req);
    } else {
        Node *node = (Node *)req->vec.iov_base;
        ptr__t ptr = next_node(req->key, node);
        if (node->type == LEAF) {
            req->is_value = true;
            ptr__t mask = BLK_SIZE - 1;
            req->ofs = ptr & mask;
            ptr &= (~mask);
        }
        traverse(ptr, req);
    }
}

void wait_for_completion(struct io_uring *ring, size_t *counter, size_t target) {
    do {
        traverse_complete(ring);
    } while (*counter != target);
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
        read_node(ptr, node, db_handler, NULL);
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
        read_node(ptr, node, db_handler, NULL);
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

Request *init_request(key__t key, WorkerArg *warg) {
    Request *req = (Request *)malloc(sizeof(Request));

    req->vec.iov_len = BLK_SIZE;
    if (posix_memalign((void **)&(req->vec.iov_base), BLK_SIZE, BLK_SIZE)) {
        perror("posix_memalign failed");
        exit(1);
    }

    req->key = key;
    req->warg = warg;
    req->is_value = false;
    gettimeofday(&(req->start), NULL);    

    return req;
}

void read_node(ptr__t ptr, Node *node, int db_handler, struct io_uring *ring) {
    struct iovec *vec = malloc(sizeof(struct iovec));
    vec->iov_base = node;
    vec->iov_len = sizeof(Node);

    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    io_uring_prep_readv(sqe, db_handler, vec, 1, decode(ptr));
    io_uring_sqe_set_data(sqe, vec);
    io_uring_submit(ring);
    
    // Debug output
    // print_node(ptr, node);
}

void read_log(ptr__t ptr, Log *log, int db_handler, struct io_uring *ring) {
    struct iovec *vec = malloc(sizeof(struct iovec));
    vec->iov_base = log;
    vec->iov_len = sizeof(Node);

    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    io_uring_prep_readv(sqe, db_handler, vec, 1, decode(ptr));
    io_uring_sqe_set_data(sqe, vec);
    io_uring_submit(ring);

    // Debug output
    // print_log(ptr, log);
}

void read_complete(struct io_uring *ring, int is_node) {
    struct io_uring_cqe *cqe;
    int ret = io_uring_wait_cqe(ring, &cqe);
    if (ret < 0) {
        perror("io_uring_wait_cqe");
        return;
    }
    if (cqe->res < 0) {
        fprintf(stderr, "[read_complete] Error in async operation: %s\n", strerror(-cqe->res));
        return;
    }

    struct iovec *vec = io_uring_cqe_get_data(cqe);
    // if (is_node) {
    //     print_node(0, (Node *)vec->iov_base);
    // } else {
    //     print_log(0, (Log *)vec->iov_base);
    // }
    free(vec);
    io_uring_cqe_seen(ring, cqe);
}

void write_node(ptr__t ptr, Node *node, int db_handler, struct io_uring *ring) {
    struct iovec *vec = malloc(sizeof(struct iovec));
    vec->iov_base = node;
    vec->iov_len = sizeof(Node);

    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    io_uring_prep_writev(sqe, db_handler, vec, 1, decode(ptr));
    io_uring_sqe_set_data(sqe, vec);
    io_uring_submit(ring);
}

void write_log(ptr__t ptr, Log *log, int db_handler, struct io_uring *ring) {
    struct iovec *vec = malloc(sizeof(struct iovec));
    vec->iov_base = log;
    vec->iov_len = sizeof(Log);

    struct io_uring_sqe *sqe = io_uring_get_sqe(ring);
    io_uring_prep_writev(sqe, db_handler, vec, 1, decode(ptr));
    io_uring_sqe_set_data(sqe, vec);
    io_uring_submit(ring);

    // Debug output
    // print_log(ptr, log);
}

void write_complete(struct io_uring *ring) {
    struct io_uring_cqe *cqe;
    int ret = io_uring_wait_cqe(ring, &cqe);
    if (ret < 0) {
        perror("io_uring_wait_cqe");
        return;
    }
    if (cqe->res < 0) {
        fprintf(stderr, "[write_complete] Error in async operation: %s\n", strerror(-cqe->res));
        return;
    }

    struct iovec *vec = io_uring_cqe_get_data(cqe);
    free(vec->iov_base);
    free(vec);
    io_uring_cqe_seen(ring, cqe);
}

int retrieve_value(ptr__t ptr, val__t val, WorkerArg *r) {
    Log *log;
    if (posix_memalign((void **)&log, 512, sizeof(Log))) {
        perror("posix_memalign failed");
        exit(1);
    }

    ptr__t mask = BLK_SIZE - 1;
    ptr__t base = ptr & (~mask);
    ptr__t offset = ptr & mask;

    read_log(base, log, r->db_handler, &(r->local_ring));
    read_complete(&(r->local_ring), 0);
    memcpy(val, log->val[offset / VAL_SIZE], VAL_SIZE);
    free(log);

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

    read_log(base, log, db_handler, NULL);
    memcpy(log->val[offset / VAL_SIZE], val, VAL_SIZE);

    int rc = 0;
    if ((rc = pthread_mutex_lock(&val_lock[log_idx])) != 0) {
        printf("lock error %d %lu\n", rc, log_idx);
    }

    write_log(base, log, db_handler, NULL);

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

    read_log(base, log, db_handler, NULL);
    size_t num = atoi(log->val[offset / VAL_SIZE]);
    sprintf(log->val[offset / VAL_SIZE], "%63lu", num / 2);
    write_log(base, log, db_handler, NULL);

    pthread_mutex_unlock(&val_lock[log_idx]);
}

ptr__t next_node(key__t key, Node *node) {
    // print_node(0, node);
    for (size_t i = 0; i < node->num; i++) {
        if (key < node->key[i]) {
            return node->ptr[i - 1];
        }
    }
    return node->ptr[node->num - 1];
}

int prompt_help() {
    printf("Usage: ./db --load number_of_layers\n");
    printf("or     ./db --run number_of_layers number_of_requests number_of_threads read_ratio rmw_ratio cache_layers\n");
    return 0;
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        return prompt_help(argc, argv);
    } else if (strcmp(argv[1], "--load") == 0) {
        return load(atoi(argv[2]));
    } else if (strcmp(argv[1], "--run") == 0) {
        if (argc < 8) {
            return prompt_help();
        }
        layer_cnt = atoi(argv[2]);
        read_ratio = atoi(argv[5]);
        rmw_ratio = atoi(argv[6]);
        cache_layer = atoi(argv[7]);
        return run(layer_cnt, atoi(argv[3]), atoi(argv[4]));
    } else {
        return prompt_help();
    }

    return 0;
}