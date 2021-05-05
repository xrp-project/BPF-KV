#include <iostream>
#include "skv_client.h"
#include "worker.h"

enum {
	PARAM_KEY_SIZE = 1,
	PARAM_VALUE_SIZE,
	PARAM_NR_ENTRY,
	PARAM_NR_THREAD,
	PARAM_FILENAME,
	PARAM_ARGC
};

int main(int argc, char *argv[]) {
	if (argc != PARAM_ARGC) {
		printf("Usage: %s <key size> <value size> <number of entries> <number of threads> <redis addr> <redis port>\n", argv[0]);
		return EINVAL;
	}
	long key_size = KEY_SIZE;
	long value_size = VAL_SIZE;
	long nr_entry = atol(argv[PARAM_NR_ENTRY]);
	int nr_thread = atol(argv[PARAM_NR_THREAD]);
	SKVFactory factory();

	run_init_workload_with_op_measurement("Initialization", &factory, nr_entry, key_size, value_size, nr_thread);
}
