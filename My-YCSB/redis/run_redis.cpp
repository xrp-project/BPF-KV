#include <iostream>
#include "worker.h"
#include "redis_client.h"

enum {
	PARAM_KEY_SIZE = 1,
	PARAM_VALUE_SIZE,
	PARAM_NR_ENTRY,
	PARAM_NR_THREAD,
	PARAM_READ_RATIO,
	PARAM_WARM_UP_OP,
	PARAM_NR_OP,
	PARAM_REDIS_ADDR,
	PARAM_REDIS_PORT,
	PARAM_ARGC
};

int main(int argc, char *argv[]) {
	if (argc != PARAM_ARGC) {
		printf("Usage: %s <key size> <value size> <number of entries> <number of threads> <read ratio> <number of warm-up ops> <number of ops> <redis addr> <redis port>\n", argv[0]);
		return EINVAL;
	}
	long key_size = atol(argv[PARAM_KEY_SIZE]);
	long value_size = atol(argv[PARAM_VALUE_SIZE]);
	long nr_entry = atol(argv[PARAM_NR_ENTRY]);
	int nr_thread = atol(argv[PARAM_NR_THREAD]);
	double read_ratio = atof(argv[PARAM_READ_RATIO]);
	long nr_warm_up_op = atol(argv[PARAM_WARM_UP_OP]);
	long nr_op = atol(argv[PARAM_NR_OP]);
	char *redis_addr = argv[PARAM_REDIS_ADDR];
	int redis_port = atoi(argv[PARAM_REDIS_PORT]);

	RedisFactory factory(redis_addr, redis_port);

	run_uniform_workload_with_op_measurement("Warm-Up", &factory, nr_entry, key_size, value_size, nr_thread,
											 read_ratio, nr_warm_up_op);
	run_uniform_workload_with_op_measurement("Uniform-Workload", &factory, nr_entry, key_size, value_size, nr_thread,
	                                         read_ratio, nr_op);
}
