ADD_EXECUTABLE(cppparquet parquet-file-driver.cc)
# Source files for main executable.
ADD_DEPENDENCIES(cppparquet parquet-thrift glog thriftstatic libcppparquet)
TARGET_LINK_LIBRARIES(cppparquet parquet-thrift glog thriftstatic libcppparquet)

ADD_EXECUTABLE(avro-schema-walker-example avro-schema-walker-example.cc)
ADD_DEPENDENCIES(avro-schema-walker-example parquet-thrift glog thriftstatic libcppparquet libavroschemawalker)
TARGET_LINK_LIBRARIES(avro-schema-walker-example parquet-thrift glog thriftstatic libcppparquet libavroschemawalker avrocpp)
