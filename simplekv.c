#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/types.h>
#include <errno.h>
#include <time.h>
#include <argp.h>

#include "simplekv.h"
#include "helpers.h"
#include "range.h"
#include "parse.h"
#include "create.h"
#include "get.h"

size_t worker_num;
size_t total_node;
size_t *layer_cap;
key__t max_key;
Node *cache;
size_t cache_cap;

const char *argp_program_version = "SimpleKV 0.1";
const char *argp_program_bug_address = "<etm2131@columbia.edu>";
static char doc[] =
"SimpleKV Benchmark for Oliver XRP Kernel\n\nCommands: create, get, range\v\
This utility provides several tools for testing and benchmarking \
SimpleKV database files on XRP enabled kernels. \
\n\nIf you are using XRP eBPF functions it is your responsibility to ensure \
the correct function is loaded before executing your query with SimpleKV. \
SimpleKV currently DOES NOT verify that the correct eBPF is loaded.";


int get_handler(char *db_path, int flag) {
    int fd = open(db_path, flag | O_DIRECT, 0655);
    if (fd < 0) {
        printf("Failed to open file %s!\n", db_path);
        exit(1);
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
    /* NOTE: this is actually 1 past the last key, since the keys start at 0 */
    max_key = layer_cap[layer_num - 1] * NODE_CAPACITY;

    cache_cap = 0;

    printf("%lu blocks in total, max key is %lu\n", total_node, max_key);

    return db;
}

/* Cache the first [layer_num] layers of the tree */
void build_cache(int db_fd, size_t layer_num, size_t cache_level) {
    /* Cache level cannot exceed min(layer_num, 3) */
    cache_level = cache_level > layer_num ? layer_num : cache_level;
    cache_level = cache_level > 3 ? 3 : cache_level;

    /* NB: This is a hack, but since we have global variables we need this */
    if (cache_level == 0) {
        cache = malloc(BLK_SIZE);
        return;
    }

    size_t entry_num = 0;
    for (size_t i = 0; i < cache_level; i++) {
        entry_num += layer_cap[i];
    }

    if (posix_memalign((void **)&cache, 512, entry_num * sizeof(Node))) {
        perror("posix_memalign failed");
        exit(1);
    }

    size_t head = 0, tail = 1;
    read_node(encode(0), &cache[head], db_fd);

    while (tail < entry_num) {
        for (size_t i = 0; i < NODE_CAPACITY; i++) {
            read_node(cache[head].ptr[i], &cache[tail], db_fd);
            cache[head].ptr[i] = (ptr__t)(&cache[tail]); // in-memory cache entry has in-memory pointer
            tail++;
        }
        head++;
    }

    cache_cap = entry_num; // enable the cache
    printf("Cache built. %lu layers %lu entries in total.\n", cache_level, entry_num);
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

void initialize_workers(WorkerArg *args, size_t total_op_count, char *db_path, int use_xrp, int bpf_fd) {
    size_t offset = 0;
    args[0].latency_arr = (size_t *) malloc(total_op_count * sizeof(size_t));
    BUG_ON(args[0].latency_arr == NULL);
    for (size_t i = 0; i < worker_num; i++) {
        args[i].index = i;
        args[i].op_count = (total_op_count / worker_num) + (i < total_op_count % worker_num);
        args[i].db_handler = get_handler(db_path, O_RDONLY);
        args[i].timer = 0;
        args[i].use_xrp = use_xrp;
        args[i].bpf_fd = bpf_fd;
        args[i].latency_arr = args[0].latency_arr + offset;
        offset += args[i].op_count;
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

static void print_tail_latency(WorkerArg* args, size_t request_num) {
    size_t *latency_arr = args[0].latency_arr;
    qsort(latency_arr, request_num, sizeof(size_t), cmp);

    printf("95%%   latency: %f us\n", get_percentile(latency_arr, request_num, 0.95) / 1000);
    printf("99%%   latency: %f us\n", get_percentile(latency_arr, request_num, 0.99) / 1000);
    printf("99.9%% latency: %f us\n", get_percentile(latency_arr, request_num, 0.999) / 1000);
}

int run(char *db_path, size_t layer_num, size_t request_num, size_t thread_num, int use_xrp,
            int bpf_fd, size_t cache_level) {

    printf("Running benchmark with %ld layers, %ld requests, and %ld thread(s)\n",
                layer_num, request_num, thread_num);
    int db_fd = initialize(layer_num, RUN_MODE, db_path);
    /* Cache up to 3 layers of the B+tree */
    build_cache(db_fd, layer_num, cache_level);

    worker_num = thread_num;
    struct timespec start, end;
    pthread_t tids[worker_num];
    WorkerArg args[worker_num];

    initialize_workers(args, request_num, db_path, use_xrp, bpf_fd);

    clock_gettime(CLOCK_REALTIME, &start);
    srandom(start.tv_nsec ^ start.tv_sec);
    start_workers(tids, args);
    terminate_workers(tids, args);
    clock_gettime(CLOCK_REALTIME, &end);

    long total_latency = 0;
    for (size_t i = 0; i < worker_num; i++) total_latency += args[i].timer;
    long run_time = 1000000000 * (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec);

    printf("Average throughput: %f op/s latency: %f usec\n", 
            (double)request_num / run_time * 1000000000, (double)total_latency / request_num / 1000);
    print_tail_latency(args, request_num);

    size_t num_extreme_latency = 0;
    for (size_t i = 0; i < request_num; ++i) {
        if (args[0].latency_arr[i] >= 1000000) {
            ++num_extreme_latency;
        }
    }
    printf("Percentage of requests with latency >= 1ms: %.4f%%\n",
           (100.0 * (double) num_extreme_latency) / ((double) request_num));

    free(args[0].latency_arr);

    return terminate();
}

void *subtask(void *args) {
    WorkerArg *r = (WorkerArg*)args;
    struct timespec tps, tpe;
    srand(r->index);
    printf("thread %ld op_count %ld\n", r->index, r->op_count);
    for (size_t i = 0; i < r->op_count; i++) {
        key__t key = random() % max_key;

        /* Time and execute the XRP lookup */
        clock_gettime(CLOCK_REALTIME, &tps);

        struct Query query = new_query(key);

        ptr__t index_offset = ROOT_NODE_OFFSET;
        /* Use the cache, if it's set */
        if (cache_cap > 0) {
            index_offset = (ptr__t) (&cache[0]);
            do {
                index_offset = nxt_node(key, (Node *) index_offset);
            } while (!is_file_offset(index_offset));
            index_offset = decode(index_offset);
        }

        long retval;
        if (r->use_xrp) {
            retval = lookup_bpf(r->db_handler, r->bpf_fd, &query, ROOT_NODE_OFFSET);
        } else {
            retval = lookup_key_userspace(r->db_handler, &query, index_offset);
        }

        clock_gettime(CLOCK_REALTIME, &tpe);
        size_t latency = 1000000000 * (tpe.tv_sec - tps.tv_sec) + (tpe.tv_nsec - tps.tv_nsec);
        r->timer += latency;
        r->latency_arr[i] = latency;

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

void read_node(ptr__t ptr, Node *node, int db_handler) {
    checked_pread(db_handler, node, sizeof(Node), decode(ptr));
}


static int parse_opt(int key, char *arg, struct argp_state *state) {
    struct ArgState *st = state->input;
    switch (key) {
        case ARGP_KEY_ARG:
        switch (state->arg_num) {
            /* DB filename */
            case 0:
                st->filename = arg;
                break;

            /* Number of layers in db */
            case 1: {
                char *endptr = NULL;
                st->layers = (int) strtol(arg, &endptr, 10);
                if ((endptr != NULL && *endptr != '\0') || st->layers < 0) {
                    argp_failure(state, 1, 0, "invalid number of layers");
                }
                break;
            }

            /* command name */
            case 2:
                if (strncmp(arg, RANGE_CMD, sizeof(RANGE_CMD)) == 0) {
                    st->subcommand_retval = run_subcommand(state, RANGE_CMD, do_range_cmd);
                }
                else if (strncmp(arg, CREATE_CMD, sizeof(CREATE_CMD)) == 0) {
                    st->subcommand_retval = run_subcommand(state, CREATE_CMD, do_create_cmd);
                }
                else if (strncmp(arg, GET_CMD, sizeof(GET_CMD)) == 0) {
                    st->subcommand_retval = run_subcommand(state, GET_CMD, do_get_cmd);
                }
                else {
                    argp_error(state, "unsupported argument %s", arg);
                }
                break;

            default:
                argp_error(state, "too many arguments");
        }
        break;

        case ARGP_KEY_END:
            if (state->arg_num < 3) {
                printf("nargs %d\n", state->arg_num);
                argp_error(state, "too few arguments");
            }
            break;

        default:
            break;
    }
    return 0;
}

int main(int argc, char *argv[]) {
    struct argp_option options[] = {
        { 0 }
    };
    struct ArgState arg_state = default_argstate();
    struct argp argp = { options, parse_opt, "DB_NAME N_LAYERS CMD [CMD_ARGS] [CMD_OPTS]", doc };
    argp_parse(&argp, argc, argv, ARGP_IN_ORDER, 0, &arg_state);

    return arg_state.subcommand_retval;
}
