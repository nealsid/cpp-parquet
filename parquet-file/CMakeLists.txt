#
# Build CMakeFile for cpp-parquet

ADD_LIBRARY(libcppparquet parquet-file.cc parquet-column.cc)

ExternalProject_Get_Property(googletest SOURCE_DIR)
INCLUDE_DIRECTORIES(${SOURCE_DIR} ${SOURCE_DIR}/include)

ADD_EXECUTABLE(parquet-file-test parquet-file-test.cc ${SOURCE_DIR}/src/gtest-all.cc)
ADD_DEPENDENCIES(parquet-file-test libcppparquet)
TARGET_LINK_LIBRARIES(parquet-file-test libcppparquet glog thrift parquet-thrift)
