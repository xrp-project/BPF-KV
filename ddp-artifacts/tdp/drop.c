#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>

SEC("ddp_pass")
int  ddp_pass_func(char *ctx)
{
	return 20;
}
