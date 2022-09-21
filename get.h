#ifndef _GET_H_
#define _GET_H_

#include "db_types.h"

struct ArgState;

void load_xrp_get();

void load_bpfkv_database(char* file_name);

void close_fds();

int do_get_cmd(int argc, char *argv[], struct ArgState *as);

int lookup_single_key(char *filename, long key, int use_xrp, int bpf_fd);

char *grab_value(char *file_name, unsigned long key, int use_xrp, int bpf_fd, ptr__t index_offset);

char *server_grab_value(unsigned long key, int use_xrp, ptr__t index_offset);

long lookup_key_userspace(int db_fd, struct Query *query, ptr__t index_offset);

void read_value_the_hard_way(int fd, char *retval, ptr__t ptr);

#endif /* _GET_H_ */
