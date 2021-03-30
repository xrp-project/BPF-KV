# SimpleKV

* Build
```
$ ./build.sh
or
$ gcc -o db db.c db.h -lpthread -D_GNU_SOURCE
```

* Run
```
$ ./db --load number_of_layers
$ ./db --run number_of_layers number_of_requests num_of_threads
```
