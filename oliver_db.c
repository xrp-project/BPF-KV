#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <errno.h>

#include "oliver_db.h"
#include "helpers.h"

int get_handler(char *db_path, int flag) {
    int fd = open(db_path, flag | O_DIRECT, 0755);
    if (fd < 0) {
        printf("Fail to open file %s!\n", db_path);
        exit(0);
    }
    return fd;
}

/* TODO (etm): Probably want to delete this... seems to be unused */
int get_log_handler(int flag) {
    int fd = open(LOG_PATH, flag | O_DIRECT, 0755);
    if (fd < 0) {
        printf("Fail to open file %s!\n", LOG_PATH);
        exit(0);
    }
    return fd;
}

/* Open database and logfile; calculate number of nodes per layer of B+tree */
int initialize(size_t layer_num, int mode, char *db_path) {
    int db;
    if (mode == LOAD_MODE) {
        db = get_handler(db_path, O_CREAT|O_TRUNC|O_WRONLY);
    } else {
        db = get_handler(db_path, O_RDONLY);
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

    return db;
}

/* Cache the first [layer_num] layers of the tree */
void build_cache(int db_fd, size_t layer_num) {
    size_t entry_num = 0;
    for (size_t i = 0; i < layer_num; i++) {
        entry_num += layer_cap[i];
    }
    
    if (posix_memalign((void **)&cache, 512, entry_num * sizeof(Node))) {
        perror("posix_memalign failed");
        exit(1);
    }

    size_t head = 0, tail = 1;
    read_node(encode(total_node * sizeof(Node)), &cache[head], db_fd);

    while (tail < entry_num) {
        for (size_t i = 0; i < cache[head].num; i++) {
            read_node(cache[head].ptr[i], &cache[tail], db_fd);
            cache[head].ptr[i] = (ptr__t)(&cache[tail]); // in-memory cache entry has in-memory pointer
            tail++;
        }
        head++;
    }

    cache_cap = entry_num; // enable the cache
    printf("Cache built. %lu layers %lu entries in total.\n", layer_num, entry_num);
}

void free_globals(void) {
    free(layer_cap);
    free(cache);
}

int terminate(void) {
    printf("Done!\n");
    free_globals();
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

int load(size_t layer_num, char *db_path) {
    printf("Load the database of %lu layers\n", layer_num);
    int db = initialize(layer_num, LOAD_MODE, db_path);

    // 1. Load the index
    Node *node;
    if (posix_memalign((void **)&node, 512, sizeof(Node))) {
        perror("posix_memalign failed");
        close(db);
        free_globals();
        exit(1);
    }
    /* 
    Disk layout:
    B+ tree nodes, each with 31 keys and 31 associated block offsets to other nodes
    Nodes are written by level in order, so, the root is first, followed by all nodes on the second level.
    Since each node has pointers to 31 other nodes, fanout is 31
    | 0  - 1  2  3  4 ... 31 - .... | ### LOG DATA ### |

    Leaf nodes have pointers into the log data, which is appended as a "heap" in the same file
    at the end of the B+tree. Once we reach a leaf node, we scan through its keys and if one matches
    the key we need, we read the offset into the heap and can retrieve the value.
    */
    ptr__t next_pos = 1;
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
        }
    }

    // 2. Load the value log
    Log *log;
    if (posix_memalign((void **)&log, 512, sizeof(Log))) {
        perror("posix_memalign failed");
        close(db);
        free(node);
        free_globals();
        exit(1);
    }
    for (size_t i = 0; i < max_key; i += LOG_CAPACITY) {
        for (size_t j = 0; j < LOG_CAPACITY; j++) {
            sprintf((char *) log->val[j], "%63lu", i + j);
        }
        write(db, log, sizeof(Log));
    }

    free(log);
    free(node);
    close(db);
    return terminate();
}

void initialize_workers(WorkerArg *args, size_t total_op_count, char *db_path) {
    for (size_t i = 0; i < worker_num; i++) {
        args[i].index = i;
        args[i].op_count = (total_op_count / worker_num) + (i < total_op_count % worker_num);
        args[i].db_handler = get_handler(db_path, O_RDONLY);
        args[i].log_handler = get_log_handler(O_RDONLY);
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

int run(char *db_path, size_t layer_num, size_t request_num, size_t thread_num) {
    printf("Run the test of %lu requests\n", request_num);
    int db_fd = initialize(layer_num, RUN_MODE, db_path);
    /* Cache up to 3 layers of the B+tree */
    build_cache(db_fd, layer_num > 3 ? 3 : layer_num);

    worker_num = thread_num;
    struct timespec start, end;
    pthread_t tids[worker_num];
    WorkerArg args[worker_num];

    initialize_workers(args, request_num, db_path);

    clock_gettime(CLOCK_REALTIME, &start);
    start_workers(tids, args);
    terminate_workers(tids, args);
    clock_gettime(CLOCK_REALTIME, &end);

    long total_latency = 0;
    for (size_t i = 0; i < worker_num; i++) total_latency += args[i].timer;
    long run_time = 1000000000 * (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec);

    printf("Average throughput: %f op/s latency: %f usec\n", 
            (double)request_num / run_time * 1000000000, (double)total_latency / request_num / 1000);

    return terminate();
}

void *subtask(void *args) {
    WorkerArg *r = (WorkerArg*)args;
    struct timespec tps, tpe;
    srand(r->index);
    printf("thread %ld op_count %ld\n", r->index, r->op_count);
    for (size_t i = 0; i < r->op_count; i++) {
        key__t key = rand() % max_key;

        /* Time and execute the XRP lookup */
        clock_gettime(CLOCK_REALTIME, &tps);

        struct Query query = new_query(key);
        long retval = lookup_bpf(r->db_handler, &query);
        
        clock_gettime(CLOCK_REALTIME, &tpe);
        r->timer += 1000000000 * (tpe.tv_sec - tps.tv_sec) + (tpe.tv_nsec - tps.tv_nsec);

        /* Parse and check value from db */
        char buf[sizeof(val__t) + 1];
        buf[sizeof(val__t)] = '\0';
        memcpy(buf, query.value, sizeof(val__t));
        unsigned long long_val = strtoul(buf, NULL, 10);


        /* Check result, print errors, etc */
        if (retval < 0) {
            fprintf(stderr, "XRP pread failed with code %d\n", errno);
        } else if (query.found == 0) {
            fprintf(stderr, "Value for key %ld not found\n", key);
        } else if (key != long_val) {
            printf("Error! key: %lu val: %s thrd: %ld\n", key, buf, r->index);
        }      
    }
    return NULL;
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
    checked_pread(db_handler, node, sizeof(Node), decode(ptr));
}

/* TODO (etm): Probably want to delete; seems unused */
void read_log(ptr__t ptr, Log *log, int db_handler) {
    checked_pread(db_handler, log, sizeof(Log), decode(ptr));
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

int prompt_help(int argc, char **argv) {
    /* Load is a misnomer - the --load option actually writes a new database to disk */
    printf("Usage: %s --load db_path number_of_layers\n", *argv);
    printf("or     %s --run db_path number_of_layers number_of_requests number_of_threads\n", *argv);
    return 0;
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        return prompt_help(argc, argv);
    }
    
    if (strcmp(argv[1], "--load") == 0) {
        char *db_path = argv[2];
        size_t n_layers = strtol_or_exit(argv[3], "Invalid number of layers\n");
        return load(n_layers, db_path);
    }
    
    if (strcmp(argv[1], "--run") == 0) {
        if (argc < 5) {
            return prompt_help(argc, argv);
        }
        char *db_path = argv[2];
        size_t layer_num, request_num, thread_num;
        layer_num = strtol_or_exit(argv[3], "Invalid number of layers\n");
        request_num = strtol_or_exit(argv[3], "Invalid number of requests\n");
        thread_num = strtol_or_exit(argv[3], "Invalid number of threads\n");

        return run(db_path, layer_num, request_num, thread_num);
    }
    
    return prompt_help(argc, argv);
}
