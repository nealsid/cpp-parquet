# cpp-parquet

March 7th, 2015

You will need the following dependencies pre-installed:

* Thrift (which requires Boost)
* Avro (just the C++ API)
* CMake 3.0.2
From the root of this repo's clone on your machine:

```sh
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

If you have parquet-tools or parquet-mr installed (parquet-tools is a project that was merged into parquet, so your version of parquet-mr has to be somewhat recent), you can also run it like this:

```sh
$ PARQUET_DUMP_PATH=<path to parquet-dump executable> GLOG_alsologtostderr=true ./parquet-file-test
```

Which will generate 'golden files' in the current directory. Currently
these files aren't used for anything, but they can be useful to view
the output of parquet-dump on the temp files the test cases create.
In the future, they will be part of the repo and will be used to
validate each test case run by comparing each test case run with the
golden file.

Setting ``GLOG_v=3`` might produce useful output in some cases, but is
pretty verbose (i.e. each individual rep/def level).
