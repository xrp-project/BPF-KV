#include "uring.h"

/*
* This code is written in the days when io_uring-related system calls are not
* part of standard C libraries. So, we roll our own system call wrapper
* functions.
* */

int io_uring_setup(unsigned entries, struct io_uring_params *p) {
	return (int) syscall(__NR_io_uring_setup, entries, p);
}

int io_uring_enter(int ring_fd, unsigned int to_submit,
                   unsigned int min_complete, unsigned int flags) {
	return (int) syscall(__NR_io_uring_enter, ring_fd, to_submit, min_complete,
			     flags, NULL);
}

int io_uring_register(int fd, unsigned int opcode, void *arg,
                      unsigned int nr_args) {
	return (int) syscall(__NR_io_uring_register, fd, opcode, arg, nr_args);
}

/*
* io_uring requires a lot of setup which looks pretty hairy, but isn't all
* that difficult to understand. Because of all this boilerplate code,
* io_uring's author has created liburing, which is relatively easy to use.
* However, you should take your time and understand this code. It is always
* good to know how it all works underneath. Apart from bragging rights,
* it does offer you a certain strange geeky peace.
* */

int app_setup_uring(struct submitter *s) {
	struct app_io_sq_ring *sring = &s->sq_ring;
	struct app_io_cq_ring *cring = &s->cq_ring;
	struct io_uring_params p;
	void *sq_ptr, *cq_ptr;

	/*
	* We need to pass in the io_uring_params structure to the io_uring_setup()
	* call zeroed out. We could set any flags if we need to, but for this
	* example, we don't.
	* */
	memset(&p, 0, sizeof(p));
	s->ring_fd = io_uring_setup(s->batch_size, &p);
	if (s->ring_fd < 0) {
		perror("io_uring_setup");
		return 1;
	}

	/*
	* io_uring communication happens via 2 shared kernel-user space ring buffers,
	* which can be jointly mapped with a single mmap() call in recent kernels.
	* While the completion queue is directly manipulated, the submission queue
	* has an indirection array in between. We map that in as well.
	* */

	int sring_sz = p.sq_off.array + p.sq_entries * sizeof(unsigned);
	int cring_sz = p.cq_off.cqes + p.cq_entries * sizeof(struct io_uring_cqe);

	/* In kernel version 5.4 and above, it is possible to map the submission and
	* completion buffers with a single mmap() call. Rather than check for kernel
	* versions, the recommended way is to just check the features field of the
	* io_uring_params structure, which is a bit mask. If the
	* IORING_FEAT_SINGLE_MMAP is set, then we can do away with the second mmap()
	* call to map the completion ring.
	* */
	if (p.features & IORING_FEAT_SINGLE_MMAP) {
		if (cring_sz > sring_sz) {
			sring_sz = cring_sz;
		}
		cring_sz = sring_sz;
	}

	/* Map in the submission and completion queue ring buffers.
	* Older kernels only map in the submission queue, though.
	* */
	sq_ptr = mmap(0, sring_sz, PROT_READ | PROT_WRITE,
		      MAP_SHARED | MAP_POPULATE,
		      s->ring_fd, IORING_OFF_SQ_RING);
	if (sq_ptr == MAP_FAILED) {
		perror("mmap");
		return 1;
	}

	if (p.features & IORING_FEAT_SINGLE_MMAP) {
		cq_ptr = sq_ptr;
	} else {
		/* Map in the completion queue ring buffer in older kernels separately */
		cq_ptr = mmap(0, cring_sz, PROT_READ | PROT_WRITE,
			      MAP_SHARED | MAP_POPULATE,
			      s->ring_fd, IORING_OFF_CQ_RING);
		if (cq_ptr == MAP_FAILED) {
			perror("mmap");
			return 1;
		}
	}
	/* Save useful fields in a global app_io_sq_ring struct for later
	* easy reference */
	sring->head = sq_ptr + p.sq_off.head;
	sring->tail = sq_ptr + p.sq_off.tail;
	sring->ring_mask = sq_ptr + p.sq_off.ring_mask;
	sring->ring_entries = sq_ptr + p.sq_off.ring_entries;
	sring->flags = sq_ptr + p.sq_off.flags;
	sring->array = sq_ptr + p.sq_off.array;

	/* Map in the submission queue entries array */
	s->sqes = mmap(0, p.sq_entries * sizeof(struct io_uring_sqe),
		       PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE,
		       s->ring_fd, IORING_OFF_SQES);
	if (s->sqes == MAP_FAILED) {
		perror("mmap");
		return 1;
	}

	/* Save useful fields in a global app_io_cq_ring struct for later
	* easy reference */
	cring->head = cq_ptr + p.cq_off.head;
	cring->tail = cq_ptr + p.cq_off.tail;
	cring->ring_mask = cq_ptr + p.cq_off.ring_mask;
	cring->ring_entries = cq_ptr + p.cq_off.ring_entries;
	cring->cqes = cq_ptr + p.cq_off.cqes;

	return 0;
}

/*
* Read from completion queue.
* In this function, we read completion events from the completion queue, get
* the data buffer that will have the file data and print it to the console.
* */
int poll_from_cq(struct submitter *s) {
	struct app_io_cq_ring *cring = &s->cq_ring;
	struct io_uring_cqe *cqe;
	unsigned head;
	int reaped = 0;

	head = *cring->head;
	read_barrier();

	while (true) {
		/*
		* Remember, this is a ring buffer. If head == tail, it means that the
		* buffer is empty.
		* */
		if (head == *cring->tail) {
			break;
		}
		read_barrier();

		/* Get the entry */
		cqe = &cring->cqes[head & *s->cq_ring.ring_mask];
		if (cqe->res != READ_SIZE) {
			printf("read_from_cq error, ret: %d\n", cqe->res);
			exit(1);
		}
		s->completion_arr[reaped++] = cqe->user_data;
		head++;
		// XXX: reap at most one
		break;
	}

	*cring->head = head;
	write_barrier();

	return reaped;
}

/*
* Submit to submission queue.
* In this function, we submit requests to the submission queue. You can submit
* many types of requests. Ours is going to be the read_xrp() request, which we
* specify via IORING_OP_READ_XRP.
*
* */
void submit_to_sq(struct submitter *s, unsigned long long user_data,
                  void *iovec, unsigned long offset, void *scratch_buffer,
                  int bpf_fd, bool with_barrier) {
	struct app_io_sq_ring *sring = &s->sq_ring;
	unsigned index = 0, tail = 0, next_tail = 0;

	/* Add our submission queue entry to the tail of the SQE ring buffer */
	next_tail = tail = *sring->tail;
	next_tail++;
	index = tail & *s->sq_ring.ring_mask;
	struct io_uring_sqe *sqe = &s->sqes[index];
	sqe->fd = 0;
	sqe->flags = IOSQE_FIXED_FILE;
	sqe->opcode = IORING_OP_READ_XRP;
	sqe->addr = (unsigned long) iovec;
	sqe->len = 1;
	sqe->off = offset;
	sqe->user_data = user_data;
        sqe->scratch = (unsigned long) scratch_buffer;
        sqe->bpf_fd = bpf_fd;
	sring->array[index] = index;
	tail = next_tail;

	if (with_barrier) {
		write_barrier();
	}

	/* Update the tail so the kernel can see it. */
	if (*sring->tail != tail) {
		*sring->tail = tail;
		if (with_barrier) {
			write_barrier();
		}
	}
}
