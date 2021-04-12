#include <stddef.h>
#include <linux/bpf.h>

__attribute__((section("ddp_test"), used))
int ddp_prog(char* buffer){
  return 3;
}
