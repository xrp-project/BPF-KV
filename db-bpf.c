#include "db-bpf.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>

void init_ring(struct submitter *s) {
    int ret;

    memset(s, 0, sizeof(*s));
    s->batch_size = QUEUE_DEPTH;

    ret = app_setup_uring(s);
    BUG_ON(ret != 0);
    ret = io_uring_register(s->ring_fd, IORING_REGISTER_FILES, &db, 1);
    BUG_ON(ret != 0);

    s->completion_arr = (long *) malloc(s->batch_size * sizeof(long));
    BUG_ON(s->completion_arr == NULL);
}

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
    if (layer_num == 0) return;
    
    size_t entry_num = 0;
    for (size_t i = 0; i < layer_num; i++) {
        entry_num += layer_cap[i];
    }
    
    if (posix_memalign((void **)&cache, 512, entry_num * sizeof(Node))) {
        perror("posix_memalign failed");
        exit(1);
    }

    size_t head = 0, tail = 1;
    pread_node(encode(0), &cache[head], db);
    while (tail < entry_num) {
        for (size_t i = 0; i < cache[head].num; i++) {
            pread_node(cache[head].ptr[i], &cache[tail], db);
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
            pwrite_node(encode(idx * BLK_SIZE), node, db);
            start_key += extent;
            idx++;
            free(node);

            // Sanity check
            // if (posix_memalign((void **)&tmp, 512, sizeof(Node))) {
            //     perror("posix_memalign failed");
            //     exit(1);
            // }
            // pread_node(encode(idx-1), tmp, db);
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
        pwrite_log(encode(idx * BLK_SIZE), log, db);
        idx++;
        free(log);

        // Sanity check
        // if (posix_memalign((void **)&tmp, 512, sizeof(Log))) {
        //     perror("posix_memalign failed");
        //     exit(1);
        // }
        // pread_log(encode(idx-1), tmp, db);
        // if (memcmp(log, tmp, sizeof(Log)) != 0) {
        //     printf("Wrong log!\n");
        // }
        // free(log);
        // free(tmp);
    }

    return terminate();
}

void initialize_workers(WorkerArg *args, size_t total_op_count) {
    args[0].histogram = (size_t *)malloc(total_op_count * sizeof(size_t));
    size_t offset = 0;
    for (size_t i = 0; i < worker_num; i++) {
        args[i].index = i;
        args[i].op_count = (total_op_count / worker_num) + (i < total_op_count % worker_num);
        args[i].db_handler = get_handler(O_RDWR|O_DIRECT);
        args[i].timer = 0;
        args[i].finished = 0;
        args[i].issued = 0;
        args[i].histogram = args[0].histogram + offset;
        offset += args[i].op_count;
        init_ring(&args[i].local_ring);
    }
}

void start_workers(pthread_t *tids, WorkerArg *args) {
    // pthread_create(&tids[worker_num], NULL, print_status, (void *)args);
    for (size_t i = 0; i < worker_num; i++) {
        pthread_create(&tids[i], NULL, subtask, (void*)&args[i]);
    }
}

void terminate_workers(pthread_t *tids, WorkerArg *args) {
    // pthread_join(tids[worker_num], NULL);
    for (size_t i = 0; i < worker_num; i++) {
        pthread_join(tids[i], NULL);
        close(args[i].db_handler);
    }
}

int cmp(const void *a, const void *b) {
    size_t val_a = *((const size_t *) a);
    size_t val_b = *((const size_t *) b);
    if (val_a == val_b) {
        return 0;
    } else if (val_a < val_b) {
        return -1;
    } else {
        return 1;
    }
}

static double get_percentile(size_t *latency_arr, size_t request_num, double percentile)
{
    double exact_index = ((double) (request_num - 1)) * percentile;
    double left_index = floor(exact_index);
    double right_index = ceil(exact_index);

    double left_value = (double) latency_arr[(size_t) left_index];
    double right_value = (double) latency_arr[(size_t) right_index];
    double value = left_value + (exact_index - left_index) * (right_value - left_value);
    return value;
}

void print_tail_latency(WorkerArg* args, size_t request_num) {
    size_t *latency_arr = args[0].histogram;
    qsort(latency_arr, request_num, sizeof(size_t), cmp);

    printf("95%%   latency: %f us\n", get_percentile(latency_arr, request_num, 0.95) / 1000);
    printf("99%%   latency: %f us\n", get_percentile(latency_arr, request_num, 0.99) / 1000);
    printf("99.9%% latency: %f us\n", get_percentile(latency_arr, request_num, 0.999) / 1000);
}

int run() {
    printf("Run the test of %lu requests\n", request_cnt);
    initialize(layer_cnt, RUN_MODE);
    build_cache(layer_cnt > cache_layer ? cache_layer : layer_cnt);

    bpf_fd = load_bpf_program("get.o");

    struct timespec start, end;
    pthread_t tids[worker_num + 1];
    WorkerArg args[worker_num];

    initialize_workers(args, request_cnt);

    clock_gettime(CLOCK_TYPE, &start);
    start_workers(tids, args);
    terminate_workers(tids, args);
    clock_gettime(CLOCK_TYPE, &end);

    long total_latency = 0;
    for (size_t i = 0; i < worker_num; i++) total_latency += args[i].timer;
    long run_time = 1000000000 * (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec);

    printf("Average throughput: %f op/s latency: %f usec\n", 
            (double)request_cnt / run_time * 1000000000, (double)total_latency / request_cnt / 1000);
    print_tail_latency(args, request_cnt);

    free(args[0].histogram);
    return terminate();
}

inline size_t get_nano(struct timespec ts) {
    return 1000000000 * ts.tv_sec + ts.tv_nsec;
}

void add_nano_to_timespec(struct timespec *x, size_t nano) {
    x->tv_sec += (x->tv_nsec + nano) / 1000000000;
    x->tv_nsec = (x->tv_nsec + nano) % 1000000000;
}

static inline bool early_than(struct timespec *x, struct timespec *y) {
    return x->tv_sec < y->tv_sec || (x->tv_sec == y->tv_sec && x->tv_nsec < y->tv_nsec);
}

void *subtask(void *args) {
    WorkerArg *r = (WorkerArg*)args;
    struct timespec start, end;

    printf("thread %ld op_count %ld\n", r->index, r->op_count);
    Request *req_arr = (Request *) malloc(QUEUE_DEPTH * sizeof(*req_arr));
    for (int i = 0; i < QUEUE_DEPTH; ++i) {
        Request *req = &req_arr[i];
        req->vec.iov_len = BLK_SIZE;
        req->vec.iov_base = aligned_alloc(PAGE_SIZE, PAGE_SIZE);
        BUG_ON(req->vec.iov_base == NULL);

        req->scratch_buffer = (uint8_t *) aligned_alloc(PAGE_SIZE, PAGE_SIZE);
        BUG_ON(req->scratch_buffer == NULL);

        req->warg = r;
        req->index = i;
    }
    int *req_idx_arr = (int *) malloc(QUEUE_DEPTH * sizeof(*req_idx_arr));
    for (int i = 0; i < QUEUE_DEPTH; ++i) {
        req_idx_arr[i] = i;
    }
    int req_idx_head = 0, req_idx_tail = QUEUE_DEPTH - 1;

    struct timespec now, deadline;
    clock_gettime(CLOCK_TYPE, &now);
    size_t gap = 1000000000 / req_per_sec;
    deadline = now;
    size_t num_submitted_sum = 0;
    size_t num_submitted_count = 0;
    int seed = r->index;

    while (r->issued < r->op_count) {
        // Submit a batch of requests
        size_t num_req_submitted = 0;
        while ((r->issued < r->op_count) && (r->issued - r->finished < QUEUE_DEPTH))
        {
            // Running late. Submit one request.
            key__t key = rand_r(&seed) % max_key;
            val__t val;

            Request *req = &req_arr[req_idx_arr[req_idx_head]];
            req_idx_head = (req_idx_head + 1) % QUEUE_DEPTH;
            req->key = key;
            clock_gettime(CLOCK_TYPE, &(req->start));

            traverse(encode(0), req);
            r->issued++;
            ++num_req_submitted;

            // Update deadline
            add_nano_to_timespec(&deadline, gap);
        }
        if (num_req_submitted > 0) {
            write_barrier();
            int ret = io_uring_enter(r->local_ring.ring_fd, num_req_submitted, 0, IORING_ENTER_GETEVENTS);
            BUG_ON(ret != num_req_submitted);
            num_submitted_sum += num_req_submitted;
            num_submitted_count++;
        }
        while (r->issued - r->finished >= QUEUE_DEPTH - MIN_BATCH_SIZE) {
            int req_idx = traverse_complete(&r->local_ring);
            req_idx_tail = (req_idx_tail + 1) % QUEUE_DEPTH;
            req_idx_arr[req_idx_tail] = req_idx;
        }

        clock_gettime(CLOCK_TYPE, &now);
        while (early_than(&now, &deadline) && r->issued - r->finished > 0) {
            int req_idx = traverse_complete(&r->local_ring);
            req_idx_tail = (req_idx_tail + 1) % QUEUE_DEPTH;
            req_idx_arr[req_idx_tail] = req_idx;
            clock_gettime(CLOCK_TYPE, &now);
        }

        clock_gettime(CLOCK_TYPE, &now);
        if (early_than(&now, &deadline)) {
            size_t diff = get_nano(deadline) - get_nano(now);
            struct timespec ts;
            ts.tv_sec = diff / 1000000000;
            ts.tv_nsec = diff % 1000000000;
            nanosleep(&ts, NULL);
        }
    }
    while (r->finished < r->op_count) {
        int req_idx = traverse_complete(&r->local_ring);
        req_idx_tail = (req_idx_tail + 1) % QUEUE_DEPTH;
        req_idx_arr[req_idx_tail] = req_idx;
    }
    printf("thread %lu finishes %lu avg submitted per syscall %f\n",
           r->index, r->finished, (double) num_submitted_sum / (double) num_submitted_count);
}

void *print_status(void *args) {
    WorkerArg *r = (WorkerArg *)args;

    pthread_cond_t cond;
    pthread_mutex_t mutex;
    pthread_cond_init(&cond, NULL);
    pthread_mutex_init(&mutex, NULL);
    size_t now_op_done = 0, old_op_done = 0;
    size_t old_op[worker_num], now_op[worker_num];
    struct timespec now, deadline;
    clock_gettime(CLOCK_TYPE, &now);
    deadline = now;
    memset(old_op, 0, worker_num * sizeof(size_t));
    memset(now_op, 0, worker_num * sizeof(size_t));

    while (now_op_done < request_cnt) {
        pthread_mutex_lock(&mutex);
        deadline.tv_sec++;
        pthread_cond_timedwait(&cond, &mutex, &deadline);
        pthread_mutex_unlock(&mutex);

        for (size_t i = 0; i < worker_num; i++) {
            now_op[i] = r[i].finished;
        }

        now_op_done = 0;
        for (size_t i = 0; i < worker_num; i++) {
            printf("thread %ld %lu op/s\n", i, now_op[i] - old_op[i]);
            old_op[i] = now_op[i];
            now_op_done += now_op[i];
        }

        printf("total %lu op/s now_op_done %lu\n", now_op_done - old_op_done, now_op_done);
        old_op_done = now_op_done;
    }
    pthread_cond_destroy(&cond);
    pthread_mutex_destroy(&mutex);
}

void traverse(ptr__t ptr, Request *req) {
    struct submitter *s = &(req->warg->local_ring);
    struct iovec *vec = &(req->vec);
    int fd = req->warg->db_handler;
 
    struct ScatterGatherQuery *sgq = (struct ScatterGatherQuery *) req->scratch_buffer;
    sgq->root_pointer = 0;
    sgq->value_ptr = 0;
    sgq->state_flags = 0;
    sgq->current_index = 0;
    sgq->n_keys = 1;
    sgq->keys[0] = req->key;

    submit_to_sq(s, (unsigned long long) req, vec, decode(ptr),
                 req->scratch_buffer, bpf_fd, false);
}

int traverse_complete(struct submitter *s) {
    while ((*(s->cq_ring.head)) == (*(s->cq_ring.tail))) {
        int ret;
        ret = io_uring_enter(s->ring_fd, /* to_submit */ 0, /* min_complete */ 1, IORING_ENTER_GETEVENTS);
        BUG_ON(ret != 0);
    }

    // poll_from_cq is changed to reap at most 1 completion
    int reaped = poll_from_cq(s);
    BUG_ON(reaped != 1);

    Request *req = (Request *) s->completion_arr[0];
    struct ScatterGatherQuery *sgq = (struct ScatterGatherQuery *) req->scratch_buffer;
    val__t val;
    Log *log = (Log *)req->vec.iov_base;
    // memcpy(val, sgq->values[0].value, VAL_SIZE);
    // if (req->key != atoi(val)) {
    //     printf("Errror! key: %lu val: %s\n", req->key, val);
    // }

    struct timespec end;
    clock_gettime(CLOCK_TYPE, &end);
    size_t latency = 1000000000 * (end.tv_sec - req->start.tv_sec) + (end.tv_nsec - req->start.tv_nsec);

    req->warg->histogram[req->warg->finished] = latency;
    req->warg->finished++;
    req->warg->timer += latency;
    // free(req->scratch_buffer);
    // free(log);
    // free(req);
    return req->index;
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
    if (posix_memalign((void **)&(req->vec.iov_base), PAGE_SIZE, PAGE_SIZE)) {
        perror("posix_memalign failed");
        exit(1);
    }

    req->scratch_buffer = (uint8_t *) aligned_alloc(PAGE_SIZE, PAGE_SIZE);
    BUG_ON(req->scratch_buffer == NULL);

    req->key = key;
    clock_gettime(CLOCK_TYPE, &(req->start));

    return req;
}

void pread_node(ptr__t ptr, Node *node, int db_handler) {
    _Static_assert(sizeof(Node) == 512);

    int ret;
    ret = pread(db_handler, node, sizeof(Node), decode(ptr));
    BUG_ON(ret != sizeof(Node));
    
    // Debug output
    // print_node(ptr, node);
}

void pread_log(ptr__t ptr, Log *log, int db_handler) {
    _Static_assert(sizeof(Log) == 512);

    int ret;
    ret = pread(db_handler, log, sizeof(Log), decode(ptr));
    BUG_ON(ret != sizeof(Log));

    // Debug output
    // print_log(ptr, log);
}

void pwrite_node(ptr__t ptr, Node *node, int db_handler) {
    _Static_assert(sizeof(Node) == 512);

    int ret;
    ret = pwrite(db_handler, node, sizeof(Node), decode(ptr));
    BUG_ON(ret != sizeof(Node));
}

void pwrite_log(ptr__t ptr, Log *log, int db_handler) {
    _Static_assert(sizeof(Log) == 512);

    int ret;
    ret = pwrite(db_handler, log, sizeof(Log), decode(ptr));
    BUG_ON(ret != sizeof(Log));

    // Debug output
    // print_log(ptr, log);
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

int bpf(int cmd, union bpf_attr *attr, unsigned int size) {
    return syscall(__NR_bpf, cmd, attr, size);
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

int prompt_help() {
    printf("Usage: ./db --load number_of_layers\n");
    printf("or     ./db --run number_of_layers number_of_requests number_of_threads read_ratio rmw_ratio cache_layers req_per_sec\n");
    return 0;
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        return prompt_help(argc, argv);
    } else if (strcmp(argv[1], "--load") == 0) {
        return load(atoi(argv[2]));
    } else if (strcmp(argv[1], "--run") == 0) {
        if (argc < 9) {
            return prompt_help();
        }
        layer_cnt = atoi(argv[2]);
        request_cnt = atoi(argv[3]);
        worker_num = atoi(argv[4]);
        read_ratio = atoi(argv[5]);
        rmw_ratio = atoi(argv[6]);
        cache_layer = atoi(argv[7]);
        req_per_sec = atoi(argv[8]);
        BUG_ON(read_ratio != 100);
        return run();
    } else {
        return prompt_help();
    }

    return 0;
}
