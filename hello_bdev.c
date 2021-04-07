/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "spdk/stdinc.h"
#include "spdk/thread.h"
#include "spdk/bdev.h"
#include "spdk/env.h"
#include "spdk/event.h"
#include "spdk/log.h"
#include "spdk/string.h"
#include "db.h"
#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>

static char *g_bdev_name = "NVMe2n1";
int global = 0;

/*
 * We'll use this struct to gather housekeeping hello_context to pass between
 * our events and callbacks.
 */
struct hello_context_t {
	struct spdk_bdev *bdev;
	struct spdk_bdev_desc *bdev_desc;
	struct spdk_io_channel *bdev_io_channel;
	Node *buff;
	char *bdev_name;
	struct spdk_bdev_io_wait_entry bdev_io_wait;
};

/*
 * Usage function for printing parameters that are specific to this application
 */
static void
hello_bdev_usage(void)
{
	printf(" -b <bdev>                 name of the bdev to use\n");
}

/*
 * This function is called to parse the parameters that are specific to this application
 */
static int hello_bdev_parse_arg(int ch, char *arg)
{
	switch (ch) {
	case 'b':
		g_bdev_name = arg;
		break;
	default:
		return -EINVAL;
	}
	return 0;
}

/*
 * Callback function for read io completion.
 */
static void
read_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct hello_context_t *hello_context = cb_arg;

	if (success) {
		// SPDK_NOTICELOG("Read string from bdev : %s\n", hello_context->buff);
		printf("num: %lu\n", hello_context->buff->num);
		// for (size_t i = 0; i < 31; i++) {
		// 	printf("key: %lu ptr: %lu\n", hello_context->buff->key[i], hello_context->buff->ptr[i]);
		// }
	} else {
		SPDK_ERRLOG("bdev io read error\n");
	}

	__atomic_fetch_add(&global, 1, __ATOMIC_SEQ_CST);

	if (global == 100) {
		/* Complete the bdev io and close the channel */
		spdk_bdev_free_io(bdev_io);
		spdk_put_io_channel(hello_context->bdev_io_channel);
		spdk_bdev_close(hello_context->bdev_desc);
		SPDK_NOTICELOG("Stopping app\n");
		spdk_app_stop(success ? 0 : -1);
	}
}

static void
hello_read(void *arg)
{
	struct hello_context_t *hello_context = arg;
	int rc = 0;
	uint32_t length = spdk_bdev_get_block_size(hello_context->bdev);

	SPDK_NOTICELOG("Reading io\n");
	rc = spdk_bdev_read(hello_context->bdev_desc, hello_context->bdev_io_channel,
			    hello_context->buff, 512 * hello_context->buff->type, length, read_complete, hello_context);

	if (rc == -ENOMEM) {
		SPDK_NOTICELOG("Queueing io\n");
		/* In case we cannot perform I/O now, queue I/O */
		hello_context->bdev_io_wait.bdev = hello_context->bdev;
		hello_context->bdev_io_wait.cb_fn = hello_read;
		hello_context->bdev_io_wait.cb_arg = hello_context;
		spdk_bdev_queue_io_wait(hello_context->bdev, hello_context->bdev_io_channel,
					&hello_context->bdev_io_wait);
	} else if (rc) {
		SPDK_ERRLOG("%s error while reading from bdev: %d\n", spdk_strerror(-rc), rc);
		spdk_put_io_channel(hello_context->bdev_io_channel);
		spdk_bdev_close(hello_context->bdev_desc);
		spdk_app_stop(-1);
	}
}

/*
 * Callback function for write io completion.
 */
static void
write_complete(struct spdk_bdev_io *bdev_io, bool success, void *cb_arg)
{
	struct hello_context_t *hello_context = cb_arg;
	uint32_t length;

	if (success) {
		SPDK_NOTICELOG("bdev io write completed successfully %lu\n", hello_context->buff->num);
	} else {
		SPDK_ERRLOG("bdev io write error: %d\n", EIO);
		spdk_put_io_channel(hello_context->bdev_io_channel);
		spdk_bdev_close(hello_context->bdev_desc);
		spdk_app_stop(-1);
		return;
	}

	/* Zero the buffer so that we can use it for reading */
	// length = spdk_bdev_get_block_size(hello_context->bdev);
	// memset(hello_context->buff, 0, length);
	// spdk_dma_free(hello_context->buff);

	__atomic_fetch_add(&global, 1, __ATOMIC_SEQ_CST);

	if (global == 100) {
		/* Complete the bdev io and close the channel */
		spdk_bdev_free_io(bdev_io);
		spdk_put_io_channel(hello_context->bdev_io_channel);
		spdk_bdev_close(hello_context->bdev_desc);
		SPDK_NOTICELOG("Stopping app\n");
		spdk_app_stop(0);
	} else {
		SPDK_NOTICELOG("global %d\n", global);
	}

	// hello_read(hello_context);
}

static void
hello_write(void *arg)
{
	struct hello_context_t *hello_context = arg;
	int rc = 0;
	uint32_t length = spdk_bdev_get_block_size(hello_context->bdev);
	SPDK_NOTICELOG("Writing to the bdev %lu\n", hello_context->buff->num);
	rc = spdk_bdev_write(hello_context->bdev_desc, hello_context->bdev_io_channel,
			     hello_context->buff, 512 * hello_context->buff->num, length, write_complete, hello_context);

	if (rc == -ENOMEM) {
		SPDK_NOTICELOG("Queueing io\n");
		/* In case we cannot perform I/O now, queue I/O */
		hello_context->bdev_io_wait.bdev = hello_context->bdev;
		hello_context->bdev_io_wait.cb_fn = hello_write;
		hello_context->bdev_io_wait.cb_arg = hello_context;
		spdk_bdev_queue_io_wait(hello_context->bdev, hello_context->bdev_io_channel,
					&hello_context->bdev_io_wait);
	} else if (rc) {
		SPDK_ERRLOG("%s error while writing to bdev: %d\n", spdk_strerror(-rc), rc);
		spdk_put_io_channel(hello_context->bdev_io_channel);
		spdk_bdev_close(hello_context->bdev_desc);
		spdk_app_stop(-1);
	}
}

static void
hello_bdev_event_cb(enum spdk_bdev_event_type type, struct spdk_bdev *bdev,
		    void *event_ctx)
{
	SPDK_NOTICELOG("Unsupported bdev event: type %d\n", type);
}

/*
 * Our initial event that kicks off everything from main().
 */
static void
hello_start(void *arg1)
{
	struct hello_context_t *hello_context = arg1;
	uint32_t blk_size, buf_align;
	int rc = 0;
	hello_context->bdev = NULL;
	hello_context->bdev_desc = NULL;

	SPDK_NOTICELOG("Successfully started the application\n");

	/*
	 * There can be many bdevs configured, but this application will only use
	 * the one input by the user at runtime.
	 *
	 * Open the bdev by calling spdk_bdev_open_ext() with its name.
	 * The function will return a descriptor
	 */
	SPDK_NOTICELOG("Opening the bdev %s\n", hello_context->bdev_name);
	rc = spdk_bdev_open_ext(hello_context->bdev_name, true, hello_bdev_event_cb, NULL,
				&hello_context->bdev_desc);
	if (rc) {
		SPDK_ERRLOG("Could not open bdev: %s\n", hello_context->bdev_name);
		spdk_app_stop(-1);
		return;
	}

	/* A bdev pointer is valid while the bdev is opened. */
	hello_context->bdev = spdk_bdev_desc_get_bdev(hello_context->bdev_desc);


	SPDK_NOTICELOG("Opening io channel\n");
	/* Open I/O channel */
	hello_context->bdev_io_channel = spdk_bdev_get_io_channel(hello_context->bdev_desc);
	if (hello_context->bdev_io_channel == NULL) {
		SPDK_ERRLOG("Could not create bdev I/O channel!!\n");
		spdk_bdev_close(hello_context->bdev_desc);
		spdk_app_stop(-1);
		return;
	}

	/* Allocate memory for the write buffer.
	 * Initialize the write buffer with the string "Hello World!"
	 */
	blk_size = spdk_bdev_get_block_size(hello_context->bdev);
	buf_align = spdk_bdev_get_buf_align(hello_context->bdev);
	printf("blk_size %u buf_align %u\n", blk_size, buf_align);
	printf("sizeof(Node) %u sizeof(Log) %u\n", sizeof(Node), sizeof(Log));
	hello_context->buff = spdk_dma_zmalloc(blk_size, buf_align, NULL);
	if (!hello_context->buff) {
		SPDK_ERRLOG("Failed to allocate buffer\n");
		spdk_put_io_channel(hello_context->bdev_io_channel);
		spdk_bdev_close(hello_context->bdev_desc);
		spdk_app_stop(-1);
		return;
	}
	// snprintf(hello_context->buff, blk_size, "%s", "Hello SOSP!\n");
	for (size_t i = 0; i < 100; i++) {
		struct hello_context_t *new_context = (struct hello_context_t*)malloc(sizeof(struct hello_context_t));
		new_context->bdev = hello_context->bdev;
		new_context->bdev_desc = hello_context->bdev_desc;
		new_context->bdev_io_channel = hello_context->bdev_io_channel;
		new_context->buff = (Node*)spdk_dma_zmalloc(blk_size, buf_align, NULL);
		new_context->bdev_name = hello_context->bdev_name;
		new_context->bdev_io_wait = hello_context->bdev_io_wait;
		if (!new_context->buff) {
			SPDK_ERRLOG("Failed to allocate buffer\n");
			spdk_put_io_channel(hello_context->bdev_io_channel);
			spdk_bdev_close(hello_context->bdev_desc);
			spdk_app_stop(-1);
			return;
		}
		// new_context->buff->num = i;
		// SPDK_NOTICELOG("spdk_bdev_write %d\n", i);
		// rc = spdk_bdev_write(new_context->bdev_desc, new_context->bdev_io_channel,
		// 	     new_context->buff, 512 * new_context->buff->num, blk_size, write_complete, new_context);
		rc = spdk_bdev_read(new_context->bdev_desc, new_context->bdev_io_channel,
			    new_context->buff, 512 * i, blk_size, read_complete, new_context);

		if (rc == -ENOMEM) {
			SPDK_NOTICELOG("Queueing io\n");
			/* In case we cannot perform I/O now, queue I/O */
			hello_context->bdev_io_wait.bdev = hello_context->bdev;
			hello_context->bdev_io_wait.cb_fn = hello_write;
			hello_context->bdev_io_wait.cb_arg = hello_context;
			spdk_bdev_queue_io_wait(hello_context->bdev, hello_context->bdev_io_channel,
						&hello_context->bdev_io_wait);
		} else if (rc) {
			SPDK_ERRLOG("%s error while writing to bdev: %d\n", spdk_strerror(-rc), rc);
			spdk_put_io_channel(hello_context->bdev_io_channel);
			spdk_bdev_close(hello_context->bdev_desc);
			spdk_app_stop(-1);
		}
	}
	// while (global < 100) {
	// 	sleep(1);
	// }
	// SPDK_NOTICELOG("Stopping app\n");
	// spdk_app_stop(0);
}

static void print_opts(struct spdk_app_opts opts) {
	printf("name: %s\n", opts.name);
	printf("json_config_file: %s\n", opts.json_config_file);
	printf("json_config_ignore_errors: %d\n", opts.json_config_ignore_errors);
	printf("rpc_addr: %s\n", opts.rpc_addr);
	printf("reactor_mask: %s\n", opts.reactor_mask);
	printf("tpoint_group_mask: %s\n", opts.tpoint_group_mask);
	printf("shm_id: %d\n", opts.shm_id);
	printf("enable_coredump: %d\n", opts.enable_coredump);
	printf("mem_channel: %d\n", opts.mem_channel);
	printf("main_core: %d\n", opts.main_core);
	printf("mem_size: %d\n", opts.mem_size);
	printf("no_pci: %d\n", opts.no_pci);
	printf("hugepage_single_segments: %d\n", opts.hugepage_single_segments);
	printf("unlink_hugepage: %d\n", opts.unlink_hugepage);
	printf("hugedir: %s\n", opts.hugedir);
	printf("print_level: %d\n", opts.print_level);
	printf("num_pci_addr: %lu\n", opts.num_pci_addr);
	printf("iova_mode: %s\n", opts.iova_mode);
	printf("delay_subsystem_init: %d\n", opts.delay_subsystem_init);
	printf("num_entries: %lu\n", opts.num_entries);
	printf("base_virtaddr: %lu\n", opts.base_virtaddr);
}

int
main(int argc, char **argv)
{
	struct spdk_app_opts opts = {};
	int rc = 0;
	struct hello_context_t hello_context = {};

	/* Set default values in opts structure. */
	spdk_app_opts_init(&opts, sizeof(opts));
	opts.name = "hello_bdev";

	/*
	 * Parse built-in SPDK command line parameters as well
	 * as our custom one(s).
	 */
	if ((rc = spdk_app_parse_args(argc, argv, &opts, "b:", NULL, hello_bdev_parse_arg,
				      hello_bdev_usage)) != SPDK_APP_PARSE_ARGS_SUCCESS) {
		exit(rc);
	}
	hello_context.bdev_name = g_bdev_name;

	/*
	 * spdk_app_start() will initialize the SPDK framework, call hello_start(),
	 * and then block until spdk_app_stop() is called (or if an initialization
	 * error occurs, spdk_app_start() will return with rc even without calling
	 * hello_start().
	 */
	print_opts(opts);
	rc = spdk_app_start(&opts, hello_start, &hello_context);
	if (rc) {
		SPDK_ERRLOG("ERROR starting application\n");
	}

	/* At this point either spdk_app_stop() was called, or spdk_app_start()
	 * failed because of internal error.
	 */

	/* When the app stops, free up memory that we allocated. */
	spdk_dma_free(hello_context.buff);

	/* Gracefully close out all of the SPDK subsystems. */
	spdk_app_fini();
	return rc;
}
