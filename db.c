#include "db.h"
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

Request *init_request(struct spdk_nvme_ns *ns, struct spdk_nvme_qpair *qpair, void *buff, key__t key, size_t *counter) {
    Request *req = (Request *)malloc(sizeof(Request));

    req->key = key;
    if (buff != NULL) {
        req->buff = buff;
    } else {
        req->buff = spdk_zmalloc(BLK_SIZE, BLK_SIZE, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
        if (req->buff == NULL) {
            printf("ERROR: buffer allocation failed\n");
            return NULL;
        }
    }
    req->is_value = false;
    req->ns = ns;
    req->qpair = qpair;
    req->counter = counter;

    return req;
}

void initialize(size_t layer_num, int mode) {
    layer_cap = (size_t *)malloc(layer_num * sizeof(size_t));
    total_node = 1;
    layer_cap[0] = 1;
    for (size_t i = 1; i < layer_num; i++) {
        layer_cap[i] = layer_cap[i - 1] * FANOUT;
        total_node += layer_cap[i];
    }
    max_key = layer_cap[layer_num - 1] * NODE_CAPACITY;
    cache_cap = 0;
    global_counter = (size_t *)malloc(sizeof(size_t));
    *global_counter = 0;

    printf("%lu blocks in total, max key is %lu\n", total_node, max_key);
}

void build_cache(size_t layer_num) {
    size_t entry_num = 0;
    for (size_t i = 0; i < layer_num; i++) {
        entry_num += layer_cap[i];
    }
    
	cache = spdk_zmalloc(BLK_SIZE * entry_num, BLK_SIZE, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);

    if (!cache) {
        printf("ERROR: cache allocation failed\n");
		exit(1);
	}

    size_t head = 0, tail = 1;
    size_t counter = 0;
    size_t target = entry_num;
    Request *req = init_request(global_ns, global_qpair, &cache[head], 0, global_counter);
    spdk_read(req, 0, 1, read_complete);
    wait_for_completion(global_qpair, global_counter, tail);

    while (tail < entry_num) {
        for (size_t i = 0; i < cache[head].num; i++) {
            // printf("head %lu i %lu ptr %d %d\n", head, i, cache[head].ptr[i], decode(cache[head].ptr[i]));
            req = init_request(global_ns, global_qpair, &cache[tail], 0, global_counter);
            spdk_read(req, decode(cache[head].ptr[i]) / BLK_SIZE, 1, read_complete);
            // read_node(cache[head].ptr[i], &cache[tail], db_ctx, &counter, target);
            cache[head].ptr[i] = (ptr__t)(&cache[tail]); // in-memory cache entry has in-memory pointer
            tail++;
        }
        wait_for_completion(global_qpair, global_counter, tail);
        head++;
    }

    cache_cap = entry_num; // enable the cache
    printf("Cache built. %lu layers %lu entries in total.\n", layer_num, entry_num);

    // Sanity check
    // for (size_t i = 0; i < cache_cap; i++) {
    //     print_node(i, &cache[i]);
    // }
}

void cleanup() {
	struct spdk_nvme_detach_ctx *detach_ctx = NULL;

    spdk_nvme_detach_async(global_ctrlr, &detach_ctx);

	while (detach_ctx && spdk_nvme_detach_poll_async(detach_ctx) == -EAGAIN) {
		;
	}
}

int terminate() {
    cleanup();
    printf("Done!\n");
    free(layer_cap);
    free(global_counter);
    if (cache != NULL) {
        spdk_free(cache);
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

void wait_for_completion(struct spdk_nvme_qpair *qpair, size_t *counter, size_t target) {
    do {
        int rc = spdk_nvme_qpair_process_completions(qpair, 0);
        // printf("rc %d\n", rc);
    } while (counter != NULL && *counter != target);
}

void write_complete(void *arg, const struct spdk_nvme_cpl *completion) {
    Request	*req = arg;
    spdk_free(req->buff);

	/* See if an error occurred. If so, display information
	 * about it, and set completion value so that I/O
	 * caller is aware that an error occurred.
	 */
	if (spdk_nvme_cpl_is_error(completion)) {
		spdk_nvme_qpair_print_completion(req->qpair, (struct spdk_nvme_cpl *)completion);
		fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
		fprintf(stderr, "Write I/O failed, aborting run\n");
		exit(1);
	} else {
        __atomic_fetch_add(req->counter, 1, __ATOMIC_SEQ_CST);
        free(req);
    }
}

void spdk_write(Request *req, size_t lba, size_t nlba) {
    int rc = 0;
    
    while ((rc = spdk_nvme_ns_cmd_write(req->ns, req->qpair, req->buff,
                                        lba, nlba, write_complete, req, 0)) != 0) {
        switch (rc) {
            case -ENOMEM:
                wait_for_completion(req->qpair, NULL, 0);
                break;
            default:
                printf("starting write I/O failed: %d\n", rc);
                exit(1);
        }
    }
}

int load() {
    printf("Load: %lu layers\n", layer_cnt);
    initialize(layer_cnt, LOAD_MODE);

    // 1. Load the index
    int rc = 0;
    size_t idx = 0;
    ptr__t next_pos = 1;

    // tmp_node and tmp_log are for sanity checks
    Node *tmp_node = (Node *)spdk_zmalloc(BLK_SIZE, BLK_SIZE, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    Log *tmp_log = (Log *)spdk_zmalloc(BLK_SIZE, BLK_SIZE, NULL, SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    if (tmp_node == NULL || tmp_log == NULL) {
        printf("ERROR: buffer allocation failed\n");
        return NULL;
    }

    for (size_t i = 0; i < layer_cnt; i++) {
        size_t extent = max_key / layer_cap[i], start_key = 0;
        printf("layer %lu extent %lu\n", i, extent);
        for (size_t j = 0; j < layer_cap[i]; j++) {
            ptr__t old_pos = next_pos;
            Request *req = init_request(global_ns, global_qpair, NULL, 0, global_counter);
            Node *cur_node = (Node *)req->buff;
            cur_node->num = NODE_CAPACITY;
            cur_node->type = (i == layer_cnt - 1) ? LEAF : INTERNAL;
            size_t sub_extent = extent / cur_node->num;
            for (size_t k = 0; k < cur_node->num; k++) {
                cur_node->key[k] = start_key + k * sub_extent;
                cur_node->ptr[k] = cur_node->type == INTERNAL ? 
                              encode(next_pos * BLK_SIZE) :
                              encode(total_node * BLK_SIZE + (next_pos - total_node) * VAL_SIZE);
                next_pos++;
            }
            start_key += extent;
            spdk_write(req, idx++, 1);

            // Sanity check
            // wait_for_completion(&pending_list, global_qpair);
            // req = init_request(global_ns, global_qpair, tmp_node, 0);
            // spdk_read(&pending_list, req, idx-1, 1, read_complete);
            // wait_for_completion(&pending_list, global_qpair);
            // assert(tmp_node->num == NODE_CAPACITY);
            // assert(tmp_node->type == (i == layer_cnt - 1) ? LEAF : INTERNAL);
            // for (size_t k = 0; k < tmp_node->num; k++) {
            //     assert(tmp_node->key[k] == start_key + k * sub_extent);
            //     assert(tmp_node->ptr[k] == tmp_node->type == INTERNAL ? 
            //                          encode(old_pos * BLK_SIZE) :
            //                          encode(total_node * BLK_SIZE + (old_pos - total_node) * VAL_SIZE));
            //     old_pos++;
            // }
            // print_node(idx-1, tmp_node);
        }
    }

    // 2. Load the value log
    for (size_t i = 0; i < max_key; i += LOG_CAPACITY) {
        Request *req = init_request(global_ns, global_qpair, NULL, 0, global_counter);
        Log *cur_log = (Log *)req->buff;
        for (size_t j = 0; j < LOG_CAPACITY; j++) {
            sprintf(cur_log->val[j], "%63lu", i + j);
        }
        spdk_write(req, idx++, 1);

        // Sanity check
        // wait_for_completion(&pending_list, global_qpair);
        // req = init_request(global_ns, global_qpair, tmp_log, 0);
        // spdk_read(&pending_list, req, idx-1, 1, read_complete);
        // wait_for_completion(&pending_list, global_qpair);
        // for (size_t j = 0; j < LOG_CAPACITY; j++) {
        //     assert(atoi(tmp_log->val[j]) == i+j);
        // }
        // print_log(idx-1, tmp_log);
    }
    wait_for_completion(global_qpair, global_counter, idx);

    spdk_free(tmp_node);
    spdk_free(tmp_log);
    return terminate();
	// printf("Calling spdk_app_stop\n");
    // spdk_app_stop(0);
}

void initialize_workers(WorkerArg *args, size_t total_op_count) {
    for (size_t i = 0; i < worker_num; i++) {
        args[i].index = i;
        args[i].op_count = (total_op_count / worker_num) + (i < total_op_count % worker_num);
	    args[i].qpair = NULL;
        args[i].timer = 0;
        args[i].counter = (size_t *)malloc(sizeof(size_t));
        *(args[i].counter) = 0;
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
        free(args[i].counter);
    }
}

void read_complete(void *arg, const struct spdk_nvme_cpl *completion) {
    Request	*req = arg;
    // printf("read_complete: %x\n", req);

	/* See if an error occurred. If so, display information
	 * about it, and set completion value so that I/O
	 * caller is aware that an error occurred.
	 */
	if (spdk_nvme_cpl_is_error(completion)) {
		spdk_nvme_qpair_print_completion(req->qpair, (struct spdk_nvme_cpl *)completion);
		fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
		fprintf(stderr, "Read I/O failed, aborting run\n");
		exit(1);
	} else {
        __atomic_fetch_add(req->counter, 1, __ATOMIC_SEQ_CST);
        free(req);
    }
}

void traverse_complete(void *arg, const struct spdk_nvme_cpl *completion) {
    Request	*req = arg;
    // printf("read_complete: %x\n", req);

	/* See if an error occurred. If so, display information
	 * about it, and set completion value so that I/O
	 * caller is aware that an error occurred.
	 */
	if (spdk_nvme_cpl_is_error(completion)) {
		spdk_nvme_qpair_print_completion(req->qpair, (struct spdk_nvme_cpl *)completion);
		fprintf(stderr, "I/O error status: %s\n", spdk_nvme_cpl_get_status_string(&completion->status));
		fprintf(stderr, "Traverse I/O failed, aborting run\n");
		exit(1);
	} else {
        if (req->is_value) {
            __atomic_fetch_add(req->counter, 1, __ATOMIC_SEQ_CST);
            val__t val;
            memcpy(val, ((Log *)req->buff)->val[req->ofs / VAL_SIZE], VAL_SIZE);
            if (req->key != atoi(val)) {
                printf("Error! key: %lu val: %s\n", req->key, val);
            }
            spdk_free(req->buff);
            free(req);
        } else {
            Node *node = (Node *)req->buff;
            ptr__t ptr = next_node(req->key, node);
            if (node->type == LEAF) {
                req->is_value = true;
                ptr__t mask = BLK_SIZE - 1;
                req->ofs = ptr & mask;
                ptr &= (~mask);
            }
            spdk_read(req, decode(ptr) / BLK_SIZE, 1, traverse_complete);
        }
    }
}

void spdk_read(Request *req, size_t lba, size_t nlba, spdk_nvme_cmd_cb cb_fn) {
    int rc = 0;
    
    while ((rc = spdk_nvme_ns_cmd_read(req->ns, req->qpair, req->buff,
                                       lba, nlba, cb_fn, req, 0)) != 0) {
        switch (rc) {
            case -ENOMEM:
                // printf("-ENOMEM %lu\n", *(req->counter));
                wait_for_completion(req->qpair, NULL, 0);
                break;
            default:
                printf("starting write I/O failed: %d\n", rc);
                exit(1);
        }
    }
}

// int run(size_t layer_num, size_t request_num, size_t thread_num) {
int run() {
    printf("Run: %lu layers, %lu requests, and %lu threads\n", 
                    layer_cnt, request_cnt, thread_cnt);

    initialize(layer_cnt, RUN_MODE);
    build_cache(layer_cnt > cache_layer ? cache_layer : layer_cnt);

    worker_num = thread_cnt;
    struct timeval start, end;
    pthread_t tids[worker_num];
    WorkerArg args[worker_num];

    initialize_workers(args, request_cnt);

    gettimeofday(&start, NULL);
    start_workers(tids, args);
    terminate_workers(tids, args);
    gettimeofday(&end, NULL);

    long total_latency = 0;
    for (size_t i = 0; i < worker_num; i++) total_latency += args[i].timer;
    long run_time = 1000000 * (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec);

    printf("Average throughput: %f op/s latency: %f usec\n", 
            (double)request_cnt / run_time * 1000000, (double)total_latency / request_cnt);

    return terminate();
}

void *subtask(void *args) {
    WorkerArg *r = (WorkerArg*)args;
    r->qpair = spdk_nvme_ctrlr_alloc_io_qpair(global_ctrlr, NULL, 0);
    struct timeval start, end;

    srand(r->index);
    printf("thread %ld op_count %ld\n", r->index, r->op_count);
    size_t window = 128; // half of the default queue size
    for (size_t i = 0; i < r->op_count; i++) {
        while (i - *(r->counter) > window) {
            wait_for_completion(r->qpair, NULL, 0);
        }

        key__t key = rand() % max_key;
        val__t val;

        gettimeofday(&start, NULL);
        get(key, val, r->qpair, r->counter);
        gettimeofday(&end, NULL);
        r->timer += 1000000 * (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec);
    }
    printf("wait_for_completion\n");
    wait_for_completion(r->qpair, r->counter, r->op_count);
}

int get(key__t key, val__t val, struct spdk_nvme_qpair *qpair, size_t *counter) {
    // printf("get key: %lu\n", key);
    ptr__t ptr = cache_cap > 0 ? (ptr__t)(&cache[0]) : encode(0);

    if (cache_cap > 0) {
        do {
            ptr = next_node(key, (Node *)ptr);
        } while (!is_file_offset(ptr));
    }

    Request *req = init_request(global_ns, qpair, NULL, key, counter);
    if (cache_layer == layer_cnt) {
        req->is_value = true;
        ptr__t mask = BLK_SIZE - 1;
        req->ofs = ptr & mask;
        ptr &= (~mask);
    }
    // TODO: there may be a bug when caching all index layers
    spdk_read(req, decode(ptr) / BLK_SIZE, 1, traverse_complete);

    return 0;
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

ptr__t next_node(key__t key, Node *node) {
    for (size_t i = 0; i < node->num; i++) {
        if (key < node->key[i]) {
            return node->ptr[i - 1];
        }
    }
    return node->ptr[node->num - 1];
}

void prompt_help() {
    printf("Usage: sudo ./db --mode run or load\n");
    printf("                 --layer number of layers\n");
    printf("                 --thread number of threads\n");
    printf("                 --request number of requests\n");
    exit(0);
}

void parse_args(int argc, char *argv[]) {
    if (argc % 2 != 1) {
        printf("Wrong argc %d\n", argc);
        prompt_help();
    } else {
        for (size_t i = 1; i < argc; i += 2) {
            if (strcmp(argv[i], "--mode") == 0) {
                db_mode = argv[i+1];
            } else if (strcmp(argv[i], "--layer") == 0) {
                layer_cnt = atoi(argv[i+1]);
            } else if (strcmp(argv[i], "--thread") == 0) {
                thread_cnt = atoi(argv[i+1]);
            } else if (strcmp(argv[i], "--request") == 0) {
                request_cnt = atoi(argv[i+1]);
            } else {
                printf("Unsupported option %s\n", argv[i]);
                prompt_help();
            }
        }

        if (db_mode == NULL ||
           (strcmp(db_mode, "load") && strcmp(db_mode, "run")) ||
           (strcmp(db_mode, "load") == 0 && layer_cnt == 0) ||
           (strcmp(db_mode, "run") == 0 && layer_cnt * thread_cnt * request_cnt == 0)) {
            printf("Bad options\n");
            prompt_help();
        }
    }
}

bool probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
	           struct spdk_nvme_ctrlr_opts *opts) {
	return strcmp(trid->traddr, "0000:01:00.0") == 0;
}

void attach_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
	           struct spdk_nvme_ctrlr *ctrlr, const struct spdk_nvme_ctrlr_opts *opts) {
	printf("Attached to %s\n", trid->traddr);

    global_ctrlr = ctrlr;
	global_ns = spdk_nvme_ctrlr_get_ns(ctrlr, 1);
	global_qpair = spdk_nvme_ctrlr_alloc_io_qpair(ctrlr, NULL, 0);
}

int main(int argc, char *argv[]) {
    int rc;
	struct spdk_env_opts opts;

	parse_args(argc, argv);

	spdk_env_opts_init(&opts);
	opts.name = "simple_kv";
	opts.shm_id = 0;
	if (spdk_env_init(&opts) < 0) {
		fprintf(stderr, "Unable to initialize SPDK env\n");
		return 1;
	}

	printf("Initializing NVMe Controllers\n");

	rc = spdk_nvme_probe(NULL, NULL, probe_cb, attach_cb, NULL);
	if (rc != 0) {
		fprintf(stderr, "spdk_nvme_probe() failed\n");
		cleanup();
		return 1;
	}

	if (global_ctrlr == NULL) {
		fprintf(stderr, "no NVMe controllers found\n");
		cleanup();
		return 1;
	}

	printf("Initialization complete. ctrlr: %x ns: %x\n", global_ctrlr, global_ns);
    if (strcmp(db_mode, "load") == 0) {
        load();
    } else if (strcmp(db_mode, "run") == 0) {
        run();
    }

	// hello_world();
	// cleanup();
	return 0;
}