ADD_EXECUTABLE(cppparquet parquet-file-driver.cc)
# Source files for main executable.
TARGET_LINK_LIBRARIES(cppparquet parquet-thrift glog pthread libcppparquet thriftstatic)

ADD_EXECUTABLE(avro-schema-walker-example avro-schema-walker-example.cc)
TARGET_LINK_LIBRARIES(avro-schema-walker-example glog pthread libcppparquet parquet-thrift thriftstatic libavroschemawalker avrocpp)
