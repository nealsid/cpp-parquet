# cpp-parquet

January 27th, 2015

Update build instructions and example code to reflect some API changes.  Google Test must still be fetched manually due to some CMake complexity with external deps that I haven't figured out yet.

After fetching this repo, inside the root directory:

```sh
$ mkdir -p third_party/googletest/src
$ cd third_party/googletest/src
$ svn co http://googletest.googlecode.com/svn/tags/release-1.7.0 googletest
$ cd ../../../
$ cmake .
```

## Examples

  * ``parquet-file-driver.cc`` is an example program that uses the API provided by CPP-Parquet.  Specify an output filename and the number of items.
  * ``avro-schema-walker-example.cc`` is an example program that takes an AVRO schema in JSON and translates it to the structures that this library needs to write Parquet files.
  * ``parquet-file-writer.cc`` isn't dependent on this project, but uses Parquet Thrift definitions directly to write a Parquet file - I wrote it to "learn by doing" and may be useful to read to understand Parquet itself.


### parquet-file-driver

```sh
cd examples
rm test.parquet
./cppparquet test.parquet 500
```

### avro-schema-walker

This takes a simple AVRO schema in JSON (provided in simple.json) and writes a parquet-file with the same schema.  No data is dumped, but you can view the schema using parquet-schema:

```sh
cd examples
rm test.parquet
./avro-schema-walker simple.json
parquet-schema test.parquet
```

## Unit Tests

To run unit tests with logging:

```sh
$ GLOG_v=2 GLOG_alsologtostderr=true ./parquet-file/parquet-file-test
```

Setting ``GLOG_v=3`` might produce useful output in some cases, but is pretty verbose (i.e. each individual rep/def level).
