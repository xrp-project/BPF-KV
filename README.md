# SimpleKV
SimpleKV is a simple key-value benchmark that performs simple get and range
query operations.  SimpleKV supports low-latency IO via the XRP BPF interface
and can also run using user-space IO (`pread()`) on mainline (and XRP
compatible) kernels.

For usage instructions run `./simplekv --help` after compiling.


# Building
SimpleKV is built using the provided Makefile. Compiling the simplekv binary
is as simple as running `make`. To perform IO with XRP via the `--use-xrp`
flag the corresponding BPF programs in the `xrp-bpf` directory must be
compiled.

These BPF programs require [libbpf](https://github.com/libbpf/libbpf) and an XRP compatible kernel.
Before compiling, install libbpf via your distribution's package manager or source.

To compile on an XRP compatible kernel with libbpf, run:
```
make
```

Alternatively, you can compile simplekv without the BPF programs and use userspace IO:
```
make simplekv
```

# Running

## Create a Database File
If this is your first time running SimpleKV you'll need to create a database
file first.  We recommend creating a database with at least 5 or 6 layers since
the benefits of XRP will be more apparent. Approximate database sizes for
various numbers of layers are given below as an estimate of the required disk
space.
```
4.0K	1-layer-db
80K	2-layer-db
2.4M	3-layer-db
72M	4-layer-db
2.2G	5-layer-db
68G	6-layer-db
```

To create a 6-layer database file:
```
./simplekv 6-layer-db 6 create
```

## Running the benchmark
SimpleKV supports get queries and range queries, both of which can be run with various options.
Usage and option docs can be reviewed by passing the `--help` flag to either command:
```
./simplekv 6-layer-db 6 get --help
./simplekv 6-layer-db 6 range --help
```

### Using XRP
Before running with XRP you must load the corresponding BPF function using the
`xrp_loader` program built via the default make target. Note that GET and RANGE operations
use separate BPF programs, so be sure to load the correct one.

For GET operations:
```
LD_PRELOAD="/usr/lib64/libbpf.so.0" ./xrp_loader xrp-bpf/get_op.o
```

For RANGE operations:
```
LD_PRELOAD="/usr/lib64/libbpf.so.0" ./xrp_loader xrp-bpf/range_op.o
```

NOTE: You may need to change `LD_PRELOAD` depending on where `libbpf.so.0` is located on your machine.

With the BPF program loaded, you can run a basic GET benchmark as follows:
```
./simplekv 6-layer-db 6 get --requests=100000 --use-xrp
```

### CPU Configuration
For consistent benchmark results you may need to disable CPU frequency scaling.


# Old code

The `ddp-artifacts` folder contains earlier code from the `ddp` kernel, which
was developed in earlier versions of XRP. It will eventually be removed from
the repo.

# Contact
Please reach out to Ioannis Zarkadas at `iz2175 AT columbia DOT edu` or Evan
Mesterhazy at `etm2131 AT columbia DOT edu` with questions.
