#include "worker.h"

void worker_thread_fn(Client *client, Workload *workload, OpMeasurement *measurement) {
	OperationType type;
	char *key_buffer = new char[workload->key_size];
	char *value_buffer= new char[workload->value_size];

	while (workload->has_next_op()) {
		workload->next_op(&type, key_buffer, value_buffer);
		switch (type) {
		case SET:
			client->do_set(key_buffer, value_buffer);
			break;
		case GET:
			char *value;
			client->do_get(key_buffer, &value);
			break;
		default:
			throw std::invalid_argument("invalid op type");
		}
		measurement->record_op(type);
		measurement->record_progress(1);
	}
	delete[] key_buffer;
	delete[] value_buffer;
}

void monitor_thread_fn(const char *task, OpMeasurement *measurement) {
	double rt_throughput[NR_OP_TYPE];
	double progress;
	long epoch = 0;
	for (;!measurement->finished
		;std::this_thread::sleep_for(std::chrono::seconds(1)), ++epoch) {
		measurement->get_rt_throughput(rt_throughput);
		progress = measurement->get_progress_percent();
		printf("%s (epoch %ld, progress %.2f%%): read throughput %.2lf ops/sec, write throughput %.2lf ops/sec, total throughput %.2lf ops/sec\n",
		       task, epoch, 100 * progress, rt_throughput[GET], rt_throughput[SET], rt_throughput[GET] + rt_throughput[SET]);
		std::cout << std::flush;
	}
	printf("%s overall: read throughput %.2lf ops/sec, write throughput %.2lf ops/sec, total throughput %.2lf ops/sec\n",
	       task, measurement->get_throughput(GET), measurement->get_throughput(SET),
	       measurement->get_throughput(GET) + measurement->get_throughput(SET));
	std::cout << std::flush;
}

void run_workload_with_op_measurement(const char *task, ClientFactory *factory, Workload **workload_arr, int nr_thread, long nr_op, long max_progress) {
	/* allocate resources */
	Client **client_arr = new Client *[nr_thread];
	std::thread **thread_arr = new std::thread *[nr_thread];
	OpMeasurement measurement;
	for (int thread_index = 0; thread_index < nr_thread; ++thread_index) {
		client_arr[thread_index] = factory->create_client();
	}

	/* start running workload */
	measurement.start_measure();
	measurement.set_max_progress(max_progress);
	for (int thread_index = 0; thread_index < nr_thread; ++thread_index) {
		thread_arr[thread_index] = new std::thread(worker_thread_fn, client_arr[thread_index], workload_arr[thread_index], &measurement);
	}
	std::thread stat_thread(monitor_thread_fn, task, &measurement);
	for (int thread_index = 0; thread_index < nr_thread; ++thread_index) {
		thread_arr[thread_index]->join();
	}
	measurement.finish_measure();
	stat_thread.join();

	/* cleanup */
	for (int thread_index = 0; thread_index < nr_thread; ++thread_index) {
		factory->destroy_client(client_arr[thread_index]);
		delete thread_arr[thread_index];
	}
	delete[] client_arr;
	delete[] thread_arr;
}

void run_init_workload_with_op_measurement(const char *task, ClientFactory *factory, long nr_entry, long key_size, long value_size, int nr_thread) {
	InitWorkload **workload_arr = new InitWorkload *[nr_thread];
	long nr_entry_per_thread = (nr_entry + nr_thread - 1) / nr_thread;
	for (int thread_index = 0; thread_index < nr_thread; ++thread_index) {
		long start_key = nr_entry_per_thread * thread_index;
		long end_key = std::min(nr_entry_per_thread * (thread_index + 1), nr_entry);
		workload_arr[thread_index] = new InitWorkload(end_key - start_key, start_key, key_size, value_size, thread_index);
	}

	run_workload_with_op_measurement(task, factory, (Workload **)workload_arr, nr_thread, nr_entry, nr_entry);

	for (int thread_index = 0; thread_index < nr_thread; ++thread_index) {
		delete workload_arr[thread_index];
	}
	delete[] workload_arr;
}

void run_uniform_workload_with_op_measurement(const char *task, ClientFactory *factory, long nr_entry, long key_size, long value_size,
                                              int nr_thread, double read_ratio, long nr_op) {
	UniformWorkload **workload_arr = new UniformWorkload *[nr_thread];
	for (int thread_index = 0; thread_index < nr_thread; ++thread_index) {
		workload_arr[thread_index] = new UniformWorkload(key_size, value_size, nr_entry, nr_op, read_ratio, thread_index);
	}

	run_workload_with_op_measurement(task, factory, (Workload **)workload_arr, nr_thread, nr_op, nr_thread * nr_op);

	for (int thread_index = 0; thread_index < nr_thread; ++thread_index) {
		delete workload_arr[thread_index];
	}
	delete[] workload_arr;
}

void run_zipfian_workload_with_op_measurement(const char *task, ClientFactory *factory, long nr_entry, long key_size, long value_size,
                                              int nr_thread, double read_ratio, double zipfian_constant, long nr_op) {
	ZipfianWorkload **workload_arr = new ZipfianWorkload *[nr_thread];
	printf("ZipfianWorkload: start initializing zipfian variables, might take a while\n");
	ZipfianWorkload base_workload(key_size, value_size, nr_entry, nr_op, read_ratio, zipfian_constant, 0);
	for (int thread_index = 0; thread_index < nr_thread; ++thread_index) {
		workload_arr[thread_index] = base_workload.clone(thread_index);
	}

	run_workload_with_op_measurement(task, factory, (Workload **)workload_arr, nr_thread, nr_op, nr_thread * nr_op);

	for (int thread_index = 0; thread_index < nr_thread; ++thread_index) {
		delete workload_arr[thread_index];
	}
	delete[] workload_arr;
}
