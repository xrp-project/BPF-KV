#ifndef YCSB_WORKER_H
#define YCSB_WORKER_H

#include <iostream>
#include <stdexcept>
#include <thread>
#include <chrono>
#include "measurement.h"
#include "client.h"
#include "workload.h"

void worker_thread_fn(Client *client, Workload *workload, OpMeasurement *measurement);
void monitor_thread_fn(const char *task, OpMeasurement *measurement);

void run_workload_with_op_measurement(const char *task, ClientFactory *factory, Workload **workload_arr,
                                      int nr_thread, long nr_op, long max_progress);
void run_init_workload_with_op_measurement(const char *task, ClientFactory *factory, long nr_entry, long key_size, long value_size,
                                           int nr_thread);
void run_uniform_workload_with_op_measurement(const char *task, ClientFactory *factory, long nr_entry, long key_size, long value_size,
                                              int nr_thread, double read_ratio, long nr_op);
void run_zipfian_workload_with_op_measurement(const char *task, ClientFactory *factory, long nr_entry, long key_size, long value_size,
                                              int nr_thread, double read_ratio, double zipfian_constant, long nr_op);

#endif //YCSB_WORKER_H
