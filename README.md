cpp-parquet
===========

Just playing around with writing Parquet files.  To compile, you need Thrift, Google Log, and Parquet-format.

$ g++ --std=c++11 -o parquet-file-driver parquet-column.cc parquet-file-driver.cc parquet-file.cc ../../parquet-format/generated/gen-cpp/parquet_{types,constants}.cpp -lthrift -lglog -I../../parquet-format/generated/gen-cpp/