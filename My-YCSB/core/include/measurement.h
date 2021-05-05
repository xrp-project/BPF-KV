#ifndef YCSB_MEASUREMENT_H
#define YCSB_MEASUREMENT_H

#include "workload.h"
#include <chrono>
#include <atomic>

struct OpMeasurement {
	std::atomic<long> op_count_arr[NR_OP_TYPE];
	std::chrono::steady_clock::time_point start_time;
	std::chrono::steady_clock::time_point end_time;

	std::atomic<long> rt_op_count_arr[NR_OP_TYPE];
	std::chrono::steady_clock::time_point rt_time;

	long max_progress;
	std::atomic<long> cur_progress;
	std::atomic<bool> finished;

	OpMeasurement();
	void set_max_progress(long new_max_progress);
	void start_measure();
	void finish_measure();

	void record_op(OperationType type);
	void record_progress(long progress_delta);

	long get_op_count(OperationType type);
	double get_throughput(OperationType type);
	void get_rt_throughput(double *throughput_arr);
	double get_progress_percent();
};

#endif //YCSB_MEASUREMENT_H
