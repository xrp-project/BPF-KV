#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <sys/socket.h>
#include <linux/bpf.h>


int bpf(int cmd, union bpf_attr * attr, unsigned int size);
void load_and_attach_ddp(const char* prog_path, const char* dev_path, const char* pin_path);
int get_ddp(const char* pin_path);
void detach_ddp(int prog_fd, const char* dev_path);

int bpf(int cmd, union bpf_attr * attr, unsigned int size){
  return syscall(__NR_bpf, cmd, attr, size);
}

//load ddp, attach, and pin ddp program
void load_and_attach_ddp(const char* prog_path, const char* dev_path, const char* pin_path){
  struct stat st;
  void *insns;
  union bpf_attr load_attr;
  union bpf_attr attach_attr;
  union bpf_attr pin_attr;
  int prog_fd;
  int dev_fd;
  int ret;

  prog_fd = open(prog_path, O_RDONLY);
  dev_fd = open(dev_path, O_RDONLY);
  fstat(prog_fd, &st);
  insns = mmap(0, st.st_size, PROT_READ, MAP_PRIVATE, prog_fd, 0);
  close(prog_fd);

  memset(&load_attr, 0, sizeof(load_attr));
  memset(&attach_attr, 0, sizeof(attach_attr));
  memset(&pin_attr, 0, sizeof(pin_attr));
  load_attr.prog_type = BPF_PROG_TYPE_DDP;
  load_attr.insns = (__u64) insns;
  load_attr.insn_cnt = st.st_size / sizeof(struct bpf_insn);
  load_attr.license = (__u64) "GPL";

  prog_fd = bpf(BPF_PROG_LOAD, &load_attr, sizeof(load_attr));
  printf("Prog Load FD: %d\n", prog_fd);

  attach_attr.target_fd = (__u32) dev_fd;
  attach_attr.attach_bpf_fd = (__u32) prog_fd;
  attach_attr.attach_type = (__u32) BPF_DDP;

  ret = bpf(BPF_PROG_ATTACH, &attach_attr, sizeof(attach_attr));
  printf("Prog Attach FD: %d\n", ret);

  pin_attr.pathname = (__aligned_u64) pin_path;
  pin_attr.bpf_fd = (__u32) prog_fd;

  ret = bpf(BPF_OBJ_PIN, &pin_attr, sizeof(pin_attr));
  printf("Prog Pin FD: %d\n", ret);

  munmap(insns, st.st_size);
  close(prog_fd);
  close(dev_fd);

}

//get pinned ddp program
int get_ddp(const char* pin_path){
  union bpf_attr get_attr;
  int prog_fd;

  memset(&get_attr, 0, sizeof(get_attr));
  get_attr.pathname = (__aligned_u64) pin_path;
  get_attr.file_flags = O_RDWR;

  prog_fd = bpf(BPF_OBJ_GET, &get_attr, sizeof(get_attr));
  printf("Prog Get FD: %d\n", prog_fd);
  return prog_fd;
}

//detaches ddp program
void detach_ddp(int prog_fd, const char* dev_path){
  union bpf_attr detach_attr;
  int dev_fd = open(dev_path, O_RDONLY);
  int ret;

  memset(&detach_attr, 0, sizeof(detach_attr));
  detach_attr.target_fd = (__u32) dev_fd;
  detach_attr.attach_bpf_fd = (__u32) prog_fd;
  detach_attr.attach_type = (__u32) BPF_DDP;

  ret = bpf(BPF_PROG_DETACH, &detach_attr, sizeof(detach_attr));
  printf("Prog Detach FD: %d\n", ret);
}

int main(int argc, char** argv){
  const char* prog_path;
  const char* dev_path;
  const char* pin_path;
  int prog_fd;
  int attach = 0;
  int c;

  //check for suite_name argument if provided
  while ((c = getopt (argc, argv, "ab:d:p:")) != -1){
    switch(c) {
      case 'a' : attach = 1;
                break;
      case 'b': prog_path = optarg;
                break;
      case 'd': dev_path = optarg;
                break;
      case 'p': pin_path = optarg;
                break;
      default: printf("optopt: %c\n", optopt);

    }
  }

  if (attach){
    load_and_attach_ddp(prog_path, dev_path, pin_path);
  }
  else{
    prog_fd = get_ddp(pin_path);
    detach_ddp(prog_fd, dev_path);
  }
  return 0;
}
