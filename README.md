cpp-parquet
===========

Just playing around with writing Parquet files.  

Now includes CMake build.  You should just need to do the normal CMake process:

```sh
$ mkdir build
$ cd build && cmake ../ && make
```

To run unit tests with logging:

```sh
cd build/parquet-file
GLOG_v=2 GLOG_alsologtostderr=true ./parquet-file-test 
```
