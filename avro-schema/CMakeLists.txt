#
# Build CMakeFile for libavroschemawalker

ADD_LIBRARY(libavroschemawalker avro-schema-walker.cc)

ADD_EXECUTABLE(avro-parquet-encoder avro-parquet-encoder.cc)
TARGET_LINK_LIBRARIES (avro-parquet-encoder libavroschemawalker glog libcppparquet libavroschemawalker avrocpp parquet-thrift thriftstatic pthread)
