cpp-parquet
===========

Just playing around with writing Parquet files.

NOW, includes a _broken build_, as of April 24th, 2014.  If you want a workaround, check out revision 142 into:

```cpp-parquet/build/third_party/build/googletest```

before running cmake & make.

Now includes CMake build.  You should just need to do the normal CMake process:

```sh
$ mkdir build
$ cd build && cmake ../ && make
```

To run unit tests with logging:

```sh
$ cd build/parquet-file
$ GLOG_v=2 GLOG_alsologtostderr=true ./parquet-file-test
```
