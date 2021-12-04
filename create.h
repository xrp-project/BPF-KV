#ifndef _CREATE_H_
#define _CREATE_H_

struct ArgState;

int do_create_cmd(int argc, char *argv[], struct ArgState *as);

int load(size_t layer_num, char *db_path);

#endif /* _CREATE_H_ */