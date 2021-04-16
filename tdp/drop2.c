#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>

SEC("ddp_pass")
unsigned long ddp_pass_func(struct ddp_key key)
{

	struct _Node *nd = (struct _Node*)ctx;
	if (nd->type != 1) {
		for (i = 0; i < nd->num; i++) {
			if (key < nd->key[i]) {
				return nd->ptr[i-1];
			}
		}
		return nd->ptr[nd->num - 1];
	}
	else {
		return nd->ptr[nd->num - 1];
	}
}
