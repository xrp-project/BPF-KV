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

static char doc[] =
"SimpleKV Benchmark for Oliver XRP Kernel\n\nCommands: create, get, range\v\
This utility provides several tools for testing and benchmarking \
SimpleKV database files on XRP enabled kernels. \
\n\nIf you are using XRP eBPF functions it is your responsibility to ensure \
the correct function is loaded before executing your query with SimpleKV. \
SimpleKV currently DOES NOT verify that the correct eBPF is loaded.";


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

