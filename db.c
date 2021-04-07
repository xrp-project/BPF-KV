#include "db.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h>

int get_handler(int flag) {
    int fd = open(DB_PATH, flag | O_DIRECT, 0755);
    if (fd < 0) {
        SPDK_NOTICELOG("Fail to open file %s!\n", DB_PATH);
        exit(0);
    }
    return fd;
}

void initialize(size_t layer_num, int mode) {
    if (mode == LOAD_MODE) {
        db = get_handler(O_CREAT|O_TRUNC|O_WRONLY);
    } else {
        db = get_handler(O_RDONLY);
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

    SPDK_NOTICELOG("%lu blocks in total, max key is %lu\n", total_node, max_key);
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
    SPDK_NOTICELOG("Cache built. %lu layers %lu entries in total.\n", layer_num, entry_num);
}

int terminate() {
    SPDK_NOTICELOG("Done!\n");
    free(layer_cap);
    free(cache);
    close(db);
    return 0;
}

int compare_nodes(Node *x, Node *y) {
    if (x->num != y->num) {
        SPDK_NOTICELOG("num differs %lu %lu\n", x->num, y->num);
        return 0;
    }
    if (x->type != y->type) {
        SPDK_NOTICELOG("type differs %lu %lu\n", x->type, y->type);
        return 0;
    }
    for (size_t i = 0; i < x->num; i++)
        if (x->key[i] != y->key[i] || x->ptr[i] != y->ptr[i]) {
            SPDK_NOTICELOG("bucket %lu differs x.key %lu y.key %lu x.ptr %lu y.ptr %lu\n",
                    i, x->key[i], y->key[i], x->ptr[i], y->ptr[i]);
            return 0;
        }
    return 1;
}

void bdev_event_cb(enum spdk_bdev_event_type type, struct spdk_bdev *bdev, void *event_ctx) {
	SPDK_NOTICELOG("Unsupported bdev event: type %d\n", type);
}

void init_context(Context *ctx) {
	int rc = 0;
	ctx->bdev = NULL;
	ctx->bdev_desc = NULL;

	SPDK_NOTICELOG("Opening the bdev %s\n", bdev_name);
	rc = spdk_bdev_open_ext(bdev_name, true, bdev_event_cb, NULL, &ctx->bdev_desc);
	if (rc) {
		SPDK_ERRLOG("Could not open bdev: %s\n", bdev_name);
		spdk_app_stop(-1);
		return;
	}

	ctx->bdev = spdk_bdev_desc_get_bdev(ctx->bdev_desc);

	SPDK_NOTICELOG("Opening io channel\n");
	ctx->bdev_io_channel = spdk_bdev_get_io_channel(ctx->bdev_desc);
	if (ctx->bdev_io_channel == NULL) {
		SPDK_ERRLOG("Could not create bdev I/O channel!!\n");
		spdk_bdev_close(ctx->bdev_desc);
		spdk_app_stop(-1);
		return;
	}
}

void shallow_copy_ctx(Context *from, Context *to) {
    to->bdev = from->bdev;
    to->bdev_desc = from->bdev_desc;
    to->bdev_io_channel = from->bdev_io_channel;
    to->bdev_io_wait = from->bdev_io_wait;

    u_int32_t buf_align = spdk_bdev_get_buf_align(to->bdev);
	to->node = spdk_dma_zmalloc(sizeof(Node), buf_align, NULL);
	to->log  = spdk_dma_zmalloc(sizeof(Log),  buf_align, NULL);

    if (!to->node || !to->log) {
		SPDK_ERRLOG("Failed to allocate memory\n");
		spdk_put_io_channel(to->bdev_io_channel);
		spdk_bdev_close(to->bdev_desc);
		spdk_app_stop(-1);
		return;
	}
}

void write_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg) {
	Context *ctx = cb_arg;

	// if (success) {
	// 	SPDK_NOTICELOG("bdev io write completed successfully %lu\n", ctx->node->key[0]);
	// } else {
	// 	SPDK_ERRLOG("bdev io write error: %d\n", EIO);
	// 	spdk_put_io_channel(ctx->bdev_io_channel);
	// 	spdk_bdev_close(ctx->bdev_desc);
	// 	spdk_app_stop(-1);
	// 	return;
	// }
    
	spdk_dma_free(ctx->node);
	spdk_dma_free(ctx->log);

	__atomic_fetch_add(ctx->counter, 1, __ATOMIC_SEQ_CST);

	if (*(ctx->counter) == ctx->target) {
		/* Complete the bdev io and close the channel */
        free(ctx->counter);
		spdk_bdev_free_io(bdev_io);
		spdk_put_io_channel(ctx->bdev_io_channel);
		spdk_bdev_close(ctx->bdev_desc);
		SPDK_NOTICELOG("Stopping app\n");
		spdk_app_stop(0);
	} else {
		SPDK_NOTICELOG("counter %d\n", *(ctx->counter));
	}

    free(ctx);
}

void ctx_write(void *arg) {
    Context *ctx = arg;
	int rc = spdk_bdev_write(ctx->bdev_desc, ctx->bdev_io_channel,
             ctx->node, ctx->offset, sizeof(Node), write_complete, ctx);
    if (rc == -ENOMEM) {
        SPDK_NOTICELOG("Queueing io\n");
        /* In case we cannot perform I/O now, queue I/O */
        ctx->bdev_io_wait.bdev = ctx->bdev;
        ctx->bdev_io_wait.cb_fn = ctx_write;
        ctx->bdev_io_wait.cb_arg = ctx;
        spdk_bdev_queue_io_wait(ctx->bdev, ctx->bdev_io_channel,
                    &ctx->bdev_io_wait);
    } else if (rc) {
        SPDK_ERRLOG("%s error while writing to bdev: %d\n", spdk_strerror(-rc), rc);
        spdk_put_io_channel(ctx->bdev_io_channel);
        spdk_bdev_close(ctx->bdev_desc);
        spdk_app_stop(-1);
    }
}

// int load(size_t layer_num) {
int load(void *argv) {
    SPDK_NOTICELOG("Load: %lu layers\n", layer_cnt);
    initialize(layer_cnt, LOAD_MODE);

    // 1. Load the index
    int rc = 0;
    size_t idx = 0;
    size_t target = total_node + max_key / LOG_CAPACITY;
    size_t *counter = (size_t *)malloc(sizeof(size_t));
    *counter = 0;
    ptr__t next_pos = 1, tmp_ptr = 0;

    Context *root_ctx = (Context *)malloc(sizeof(Context));
    init_context(root_ctx);

    for (size_t i = 0; i < layer_cnt; i++) {
        size_t extent = max_key / layer_cap[i], start_key = 0;
        SPDK_NOTICELOG("layer %lu extent %lu\n", i, extent);
        for (size_t j = 0; j < layer_cap[i]; j++) {
            Context *ctx = (Context *)malloc(sizeof(Context));
            shallow_copy_ctx(root_ctx, ctx);
            ctx->counter = counter;
            ctx->target = target;
            ctx->offset = sizeof(Node) * idx++;
            ctx->node->num = NODE_CAPACITY;
            ctx->node->type = (i == layer_cnt - 1) ? LEAF : INTERNAL;
            size_t sub_extent = extent / ctx->node->num;
            for (size_t k = 0; k < ctx->node->num; k++) {
                ctx->node->key[k] = start_key + k * sub_extent;
                ctx->node->ptr[k] = ctx->node->type == INTERNAL ? 
                              encode(next_pos   * BLK_SIZE) :
                              encode(total_node * BLK_SIZE + (next_pos - total_node) * VAL_SIZE);
                next_pos++;
            }
            start_key += extent;

            // write(db, node, sizeof(Node));
            ctx_write(ctx);

            // Sanity check
            // read_node(tmp_ptr, &tmp);
            // compare_nodes(&node, &tmp);
            // tmp_ptr += BLK_SIZE;
        }
    }

    // 2. Load the value log
    for (size_t i = 0; i < max_key; i += LOG_CAPACITY) {
        Context *ctx = (Context *)malloc(sizeof(Context));
        shallow_copy_ctx(root_ctx, ctx);
        ctx->counter = counter;
        ctx->target = target;
        ctx->offset = sizeof(Log) * idx++;
        for (size_t j = 0; j < LOG_CAPACITY; j++) {
            sprintf(ctx->log->val[j], "%63lu", i + j);
        }
        // write(db, log, sizeof(Log));
        ctx_write(ctx);

        // Sanity check
        // read_log((total_node + i / LOG_CAPACITY) * BLK_SIZE, &log);
    }

    // return terminate();
	// SPDK_NOTICELOG("Calling spdk_app_stop\n");
    // spdk_app_stop(0);
}

void initialize_workers(WorkerArg *args, size_t total_op_count) {
    for (size_t i = 0; i < worker_num; i++) {
        args[i].index = i;
        args[i].op_count = (total_op_count / worker_num) + (i < total_op_count % worker_num);
        args[i].db_handler = get_handler(O_RDONLY);
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

// int run(size_t layer_num, size_t request_num, size_t thread_num) {
int run(void *argv) {
    SPDK_NOTICELOG("Run: %lu layers, %lu requests, and %lu threads\n", 
                    layer_cnt, request_cnt, thread_cnt);
    spdk_app_stop(0);
    // initialize(layer_num, RUN_MODE);
    // build_cache(layer_num > 3 ? 3 : layer_num);

    // worker_num = thread_num;
    // struct timeval start, end;
    // pthread_t tids[worker_num];
    // WorkerArg args[worker_num];

    // initialize_workers(args, request_num);

    // gettimeofday(&start, NULL);
    // start_workers(tids, args);
    // terminate_workers(tids, args);
    // gettimeofday(&end, NULL);

    // long total_latency = 0;
    // for (size_t i = 0; i < worker_num; i++) total_latency += args[i].timer;
    // long run_time = 1000000 * (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec);

    // SPDK_NOTICELOG("Average throughput: %f op/s latency: %f usec\n", 
    //         (double)request_num / run_time * 1000000, (double)total_latency / request_num);

    // return terminate();
}

void *subtask(void *args) {
    WorkerArg *r = (WorkerArg*)args;
    struct timeval start, end;

    srand(r->index);
    SPDK_NOTICELOG("thread %ld op_count %ld\n", r->index, r->op_count);
    for (size_t i = 0; i < r->op_count; i++) {
        key__t key = rand() % max_key;
        val__t val;

        gettimeofday(&start, NULL);
        get(key, val, r->db_handler);
        gettimeofday(&end, NULL);
        r->timer += 1000000 * (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec);

        if (key != atoi(val)) {
            SPDK_NOTICELOG("Error! key: %lu val: %s thrd: %ld\n", key, val, r->index);
        }       
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

void print_node(ptr__t ptr, Node *node) {
    SPDK_NOTICELOG("----------------\n");
    SPDK_NOTICELOG("ptr %lu num %lu type %lu\n", ptr, node->num, node->type);
    for (size_t i = 0; i < NODE_CAPACITY; i++) {
        SPDK_NOTICELOG("(%6lu, %8lu) ", node->key[i], node->ptr[i]);
    }
    SPDK_NOTICELOG("\n----------------\n");
}

void print_log(ptr__t ptr, Log *log) {
    SPDK_NOTICELOG("----------------\n");
    SPDK_NOTICELOG("ptr %lu\n", ptr);
    for (size_t i = 0; i < LOG_CAPACITY; i++) {
        SPDK_NOTICELOG("%s\n", log->val[i]);
    }
    SPDK_NOTICELOG("\n----------------\n");
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

void prompt_help(void) {
    printf(" -M run or load\n");
    printf(" -N number of layers\n");
    printf(" -O number of requests\n");
    printf(" -T number of threads\n");
}

int parse_arg(int ch, char *arg) {
	switch (ch) {
        case 'M':
            db_mode = arg;
            break;
        case 'N':
            layer_cnt = atoi(arg);
            break;
        case 'O':
            request_cnt = atoi(arg);
            break;
        case 'T':
            thread_cnt = atoi(arg);
            break;
        default:
            return -EINVAL;
	}
	return 0;
}

int main(int argc, char *argv[]) {
    struct spdk_app_opts opts = {};
	int rc = 0;

	/* Set default values in opts structure. */
	spdk_app_opts_init(&opts, sizeof(opts));
	opts.name = "simple_kv";

    /*
	 * Parse built-in SPDK command line parameters as well
	 * as our custom one(s).
	 */
	if ((rc = spdk_app_parse_args(argc, argv, &opts, "M:N:O:T", NULL, parse_arg,
				      prompt_help)) != SPDK_APP_PARSE_ARGS_SUCCESS) {
		exit(rc);
	}

	/*
	 * spdk_app_start() will initialize the SPDK framework, call hello_start(),
	 * and then block until spdk_app_stop() is called (or if an initialization
	 * error occurs, spdk_app_start() will return with rc even without calling
	 * hello_start().
	 */
    if (strcmp(db_mode, "load") == 0 && layer_cnt > 0) {
        rc = spdk_app_start(&opts, load, NULL);
    } else if (strcmp(db_mode, "run") == 0 && layer_cnt * request_cnt * thread_cnt > 0) {
        rc = spdk_app_start(&opts, run, NULL);
    } else {
        prompt_help();
    }

	if (rc) {
		SPDK_ERRLOG("ERROR starting application\n");
	}

	/* Gracefully close out all of the SPDK subsystems. */
	spdk_app_fini();
	return rc;
}