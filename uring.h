/* built upon io_uring codebase https://github.com/shuveb/io_uring-by-example */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <sys/mman.h>
#include <sys/uio.h>
#include <linux/fs.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdbool.h>

/* If your compilation fails because the header file below is missing,
* your kernel is probably too old to support io_uring.
* */
#include <linux/io_uring.h>

/* This is x86 specific */
#define read_barrier()  __asm__ __volatile__("":::"memory")
#define write_barrier() __asm__ __volatile__("":::"memory")

#define READ_SIZE 512
#define PAGE_SIZE 4096

struct app_io_sq_ring {
	unsigned *head;
	unsigned *tail;
	unsigned *ring_mask;
	unsigned *ring_entries;
	unsigned *flags;
	unsigned *array;
};

struct app_io_cq_ring {
	unsigned *head;
	unsigned *tail;
	unsigned *ring_mask;
	unsigned *ring_entries;
	struct io_uring_cqe *cqes;
};

struct submitter {
	int ring_fd;
	struct app_io_sq_ring sq_ring;
	struct io_uring_sqe *sqes;
	struct app_io_cq_ring cq_ring;

	int batch_size;
	long *completion_arr;
};

static inline void BUG_ON(int cond) {
        if (cond)
                abort();
}

int io_uring_setup(unsigned entries, struct io_uring_params *p);

int io_uring_enter(int ring_fd, unsigned int to_submit, unsigned int min_complete, unsigned int flags);

int io_uring_register(int fd, unsigned int opcode, void *arg, unsigned int nr_args);

int app_setup_uring(struct submitter *s);

int poll_from_cq(struct submitter *s);

void submit_to_sq(struct submitter *s, unsigned long long user_data,
                  void *iovec, unsigned long offset, void *scratch_buffer,
                  int bpf_fd, bool with_barrier);
