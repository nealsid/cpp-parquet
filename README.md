cpp-parquet
===========

Just playing around with writing Parquet files.  

Now includes CMake build.  You should just need to do the normal CMake process:

```sh
$ mkdir build
$ cd build && cmake ../ && make
```

There is one hiccup with external deps, though. If you blow away the build directory, and then rereun CMake, it won't re-install Google Glog from it's build directory because the source hasn't changed.  You can just rm the `/tmp/google-glog` directory away so that it's refetched/rebuilt, or do:

```sh
cd /tmp/google-glog/src/google-glog-build/ 
make install
```
