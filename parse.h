#ifndef _PARSE_H_
#define _PARSE_H_

#include <argp.h>

#define CACHE_ARG_KEY 1337

struct ArgState {
    /* Required Args */
    char *filename;
    int layers;

    int subcommand_retval;
};

struct GetArgs {
    long key;

    /* Flags */
    int key_set;
    int xrp;

    int threads;
    int requests;
    size_t cache_level;
    size_t database_layers;
};

struct RangeArgs {
    int dump_flag;
    int xrp;
    long range_begin;
    long range_end;
    long requests;
};

static inline struct ArgState default_argstate() {
    struct ArgState as = { 0 };
    return as;
}

int run_subcommand(struct argp_state *state, char *cmd_name, int(*subcommand)(int argc, char* argv[], struct ArgState*));

void parse_range(struct argp_state *state, struct RangeArgs *st, char *range_str);

void parse_range_opts(int argc, char *argv[], struct RangeArgs *range_args);

void parse_get_opts(int argc, char *argv[], struct GetArgs *get_args);

void parse_create_opts(int argc, char *argv[]);

#endif /* _PARSE_H_ */