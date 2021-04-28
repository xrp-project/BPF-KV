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
$ ./db --load number_of_layers
$ ./db --run number_of_layers number_of_requests num_of_threads
```
