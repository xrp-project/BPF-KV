// SPDX-License-Identifier: GPL-2.0
// test ir decoder
//
// Copyright (C) 2018 Sean Young <sean@mess.org>

// A lirc chardev is a device representing a consumer IR (cir) device which
// can receive infrared signals from remote control and/or transmit IR.
//
// IR is sent as a series of pulses and space somewhat like morse code. The
// BPF program can decode this into scancodes so that rc-core can translate
// this into input key codes using the rc keymap.
//
// This test works by sending IR over rc-loopback, so the IR is processed by
// BPF and then decoded into scancodes. The lirc chardev must be the one
// associated with rc-loopback, see the output of ir-keytable(1).
//
// The following CONFIG options must be enabled for the test to succeed:
// CONFIG_RC_CORE=y
// CONFIG_BPF_RAWIR_EVENT=y
// CONFIG_RC_LOOPBACK=y

// Steps:
// 1. Open the /dev/lircN device for rc-loopback (given on command line)
// 2. Attach bpf_lirc_mode2 program which decodes some IR.
// 3. Send some IR to the same IR device; since it is loopback, this will
//    end up in the bpf program
// 4. bpf program should decode IR and report keycode
// 5. We can read keycode from same /dev/lirc device

#include <linux/bpf.h>
#include <linux/lirc.h>
#include <linux/input.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <poll.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "bpf_util.h"
#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <asm/types.h>

int main(int argc, char **argv)
{
	struct bpf_object *obj;
	int ret, lircfd, progfd, inputfd;
	int testir1 = 0x1dead;
	int testir2 = 0x20101;
	unsigned int prog_ids[10], prog_flags[10], prog_cnt;

	/*
	if (argc != 3) {
		printf("Usage: %s /dev/lircN /dev/input/eventM\n", argv[0]);
		return 2;
	}
	*/

	ret = bpf_prog_load("drop2.o",
			    BPF_PROG_TYPE_DDP, &obj, &progfd);
	if (ret) {
		printf("Failed to load bpf program\n");
		return 1;
	}
	
	lircfd = open("/dev/treenvme0", O_RDWR | O_NONBLOCK);
	ret = bpf_prog_attach(progfd, lircfd, BPF_DDP, 0);
	if (ret) {
		printf("Failed to attach bpf to lirc device: %m\n");
		return 1;
	}
	
	//ret = bpf_prog_detach2(progfd, lircfd, BPF_DDP);
	if (ret) {
		printf("bpf_prog_detach2: returned %m\n");
		return 1;
	}

	return 0;
}
