#include <stdlib.h>
#include <string.h>

#include "parse.h"
#include "helpers.h"

/* Parsing for main */


/* Parsing for DB creation */
static struct argp_option create_opts[] = {
        { 0 }
};
static char create_doc[] = "Create a new database with the specified number of layers.";

static int _parse_create_opts(int key, char *arg, struct argp_state *state) {
    if (key == CACHE_ARG_KEY) {
        argp_error(state, "unsupported argument");
    }
    return 0;
}

void parse_create_opts(int argc, char *argv[]) {
    struct argp argp = {create_opts, _parse_create_opts, "", create_doc};
    argp_parse(&argp, argc, argv, 0, 0, create_opts);
}


/* Parsing for get key benchmark */
static struct argp_option get_opts[] = {
        { "cache", CACHE_ARG_KEY, "NUM", 0, "Number of B+ tree layers to cache."
                                            " Must be less than 3 and the number of database layers." },
        { "key", 'k', "KEY", 0, "Retrieve a single key from the database." },
        { "use-xrp", 'x', 0, 0, "Use the (previously) loaded XRP BPF function to query the DB." },
        { "requests", 'r', "REQ", 0, "Number of requests to submit per thread. Ignored if -k is set." },
        { "threads" , 't', "N_THREADS", 0, "Number of concurrent threads to run. Ignored if -k is set." },
        { 0 }
};
static char get_doc[] = "Run the benchmark to retrieve single keys from the database";

static int _parse_get_opts(int key, char *arg, struct argp_state *state) {
    struct GetArgs *st = state->input;
    switch (key) {
        case CACHE_ARG_KEY: {
            char *endptr = NULL;
            unsigned long cache_level = strtoul(arg, &endptr, 10);
            if ((endptr != NULL && *endptr != '\0') || cache_level > 3) {
                argp_failure(state, 1, 0, "invalid cache level. Allowed: 0 <= level <= 3");
            }
            st->cache_level = (size_t) cache_level;
            if (cache_level > st->database_layers) {
                argp_failure(state, 1, 0, "error: cache level exceeds database layers");
            }
        }
            break;

        case 'x':
            st -> xrp = 1;
            break;

        case 'r': {
            char *endptr = NULL;
            st->requests = strtol(arg, &endptr, 10);
            if ((endptr != NULL && *endptr != '\0') || st->requests < 0) {
                argp_failure(state, 1, 0, "invalid number of requests");
            }
        }
            break;

        case 't': {
            char *endptr = NULL;
            st->threads = strtol(arg, &endptr, 10);
            if ((endptr != NULL && *endptr != '\0') || st->threads < 0) {
                argp_failure(state, 1, 0, "invalid number of threads");
            }
        }
            break;

        case 'k': {
            char *endptr = NULL;
            st->key = strtol(arg, &endptr, 10);
            st->key_set = 1;
            if (endptr != NULL && *endptr != '\0') {
                argp_failure(state, 1, 0, "invalid key");
            }
        }
            break;

        case ARGP_KEY_ARG:
            argp_error(state, "unsupported argument %s", arg);
            break;

        case ARGP_KEY_END:
            if (st->cache_level > st->database_layers) {
                argp_error(state, "number of cache layers exceeds database layers");
            }
            else if (st->cache_level == st->database_layers) {
                /*
                 * This is enforced to avoid additional refactoring to handle the case where
                 * the entire index is cached.
                 * Currently, our retreival functions assume at least one level of the index is traversed.
                 */
                argp_error(state, "number of cache layers must be less than number of database layers");
            }
            break;

        default:
            break;
    }
    return 0;
}

void parse_get_opts(int argc, char *argv[], struct GetArgs *get_args) {
    struct argp argp = {get_opts, _parse_get_opts, "", get_doc};
    argp_parse(&argp, argc, argv, 0, 0, get_args);
}


/* Parsing for range query benchmark */
static struct argp_option range_opts[] = {
        { "dump", 'd', 0, 0, "Dump values to stdout." },
        { "sum", RANGE_SUM_KEY, 0, 0, "Sum the first 8 bytes of each value instead of returning them."},
        { "use-xrp", 'x', 0, 0, "Use the (previously) loaded XRP BPF function to query the DB." },
        { "requests", 'r', "REQ", 0, "Number of requests to submit per thread. Ignored if -k is set." },
        { "range-size", 's', "SIZE", 0, "Size of randomly generated ranges for benchmarking." },
        { 0 }
};
static char range_doc[] = "Perform a range query against the specified database\v"
                          "Note: Range argument is only optional if --range-size is specified.";

static int _parse_range_opts(int key, char *arg, struct argp_state *state) {
    struct RangeArgs *st = state->input;
    switch (key) {
        case ARGP_KEY_ARG:
            switch (state->arg_num) {
                case 0: {
                    struct Range range = {0};
                    if (parse_range(&range, arg) != 0) {
                        argp_error(state, "invalid range");
                    }
                    st->range_begin = range.begin;
                    st->range_end = range.end;
                    break;
                default:
                        argp_error(state, "too many arguments");
                }
            }
            break;

        case 'd':
            st->dump_flag = 1;
            break;

        case RANGE_SUM_KEY:
            st->agg_op = AGG_SUM;
            break;

        case 'r': {
            char *endptr = NULL;
            st->requests = strtol(arg, &endptr, 10);
            if ((endptr != NULL && *endptr != '\0') || st->requests < 0) {
                argp_error(state, "invalid number of requests");
            }
        }
            break;

        case 'x':
            st->xrp = 1;
            break;

        case 's': {
            char *endptr = NULL;
            st->range_size = strtol(arg, &endptr, 10);
            if ((endptr != NULL && *endptr != '\0') || st->range_size < 0) {
                argp_error(state, "invalid number of requests");
            }
        }
            break;

        case ARGP_KEY_END:
            if (state->arg_num != 1 && st->range_size == 0) {
                argp_error(state, "no range specified");
            }
            break;

        default:
            break;
    }
    return 0;
}

void parse_range_opts(int argc, char *argv[], struct RangeArgs *range_args) {
    struct argp argp = {range_opts, _parse_range_opts, "[BEGIN,END]", range_doc};
    argp_parse(&argp, argc, argv, 0, 0, range_args);
}

int run_subcommand(struct argp_state *state, char *cmd_name, int (*subcommand)(int argc, char* argv[], struct ArgState*)) {
    int argc = state->argc - state->next + 1;
    char **argv = &state->argv[state->next - 1];
    char *argv0 = argv[0];

    /* Add two chars, one for space and one for null terminator */
    char *new_cmd_name = malloc(strlen(state->name) + strlen(cmd_name) + 2);
    if (new_cmd_name == NULL) {
        perror("malloc failed");
        exit(1);
    }
    sprintf(new_cmd_name, "%s %s", state->name, cmd_name);
    argv[0] = new_cmd_name;

    int retval = subcommand(argc, argv, state->input);

    /* free subcommand name buff */
    free(new_cmd_name);
    state->argv[0] = argv0;

    /* skip remaining parsing since subcommand has handled the rest of the args */
    state->next += argc - 1;

    return retval;
}

/**
 * Parse a string specifying a half-open range
 *
 * Example strings:
 *     10,20
 *     5,8
 *     0,100
 *
 * @param state
 * @param st
 * @param range_str
 */
int parse_range(struct Range *range, char *range_str) {
    /* Parse range query params */
    char *comma = strchr(range_str, ',');
    if (comma == NULL) {
        return -1;
    }
    *comma = '\0';

    char *endptr = NULL;
    range->begin = strtoul(range_str, &endptr, 10);
    if ((endptr != NULL && *endptr != '\0')) {
        return -1;
    }
    endptr = NULL;
    range->end = strtoul(comma + 1, &endptr, 10);
    if ((endptr != NULL && *endptr != '\0')) {
        return -1;
    }
    return 0;
}
