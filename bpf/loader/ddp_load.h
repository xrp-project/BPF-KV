
#include <bpf/bpf.h>
#include <bpf/libbpf.h>

static inline int bpf_ddp_load(struct bpf_object **obj,
				const char *path,
				const char *section_name,
				int *prog_fd)
{
	struct bpf_program *prog, *main_prog;
	int prog_array_fd;
	int ret, fd, i;

	ret = bpf_prog_load(path, BPF_PROG_TYPE_DDP, obj,
			    prog_fd);
	if (ret)
		return ret;

	main_prog = bpf_object__find_program_by_title(*obj, section_name);
	if (!main_prog)
		return -1;

	*prog_fd = bpf_program__fd(main_prog);
	if (*prog_fd < 0)
		return -1;


	return 0;
}

