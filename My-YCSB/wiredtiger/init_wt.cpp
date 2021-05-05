#include <iostream>
#include "worker.h"
#include "wt_client.h"

enum {
	PARAM_KEY_SIZE = 1,
	PARAM_VALUE_SIZE,
	PARAM_NR_ENTRY,
	PARAM_ARGC
};

int main(int argc, char *argv[]) {
	if (argc != PARAM_ARGC) {
		printf("Usage: %s <key size> <value size> <number of entries>\n", argv[0]);
		return EINVAL;
	}
	long key_size = atol(argv[PARAM_KEY_SIZE]);
	long value_size = atol(argv[PARAM_VALUE_SIZE]);
	long nr_entry = atol(argv[PARAM_NR_ENTRY]);

	WiredTigerFactory factory(nullptr, nullptr, nullptr, nullptr, nullptr, true, nullptr);

	factory.update_cursor_config(WiredTigerClient::cursor_bulk_config);
	run_init_workload_with_op_measurement("Initialization", &factory, nr_entry, key_size, value_size, 1);
}
