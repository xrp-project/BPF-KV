# SimpleKV

* Prerequisite
```
$ git clone https://github.com/axboe/liburing.git
$ cd liburing
$ ./configure
$ sudo make install
```

* Build
```
$ ./build.sh
```

* Run
```
$ ./build/db --load number_of_layers
$ ./build/db --run number_of_layers number_of_requests number_of_threads read_ratio rmw_ratio cache_layers
```
