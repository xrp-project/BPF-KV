#ifndef _RANGE_H
#define _RANGE_H

#include "db_types.h"

struct ArgState;

int do_range_cmd(int argc, char *argv[], struct ArgState*);

int submit_range_query(struct RangeQuery *query, int db_fd, int use_xrp);

int iter_print(int idx, Node *node, void *state);

typedef int(*key_iter_action)(int idx, Node *node, void *state);

int iterate_keys(char *filename, int levels, long start_key, long end_key,
                 key_iter_action fn, void *fn_state);

#endif /* _RANGE_H */