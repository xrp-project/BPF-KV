# SimpleKV

* Prerequisite

  * Setup SPDK as [Getting Started](https://spdk.io/doc/getting_started.html).

  * Update the variable `SPDK_ROOT_DIR` in `SimpleKV/Makefile`.


* Build

```
$ make
```

* Run

```
$ sudo ./db --mode load --layer number_of_index_layers
$ sudo ./db --mode run --layer number_of_index_layers --thread num_of_threads --request number_of_requests --cache cached_layers
```
