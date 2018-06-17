ExternalProject_Add(parquet-format
   PREFIX ${CMAKE_BINARY_DIR}/third_party/build/parquet-format
   GIT_REPOSITORY https://github.com/apache/parquet-format
   GIT_TAG apache-parquet-format-2.5.0
   # Set these to nonempty, so CMake executes the subsequent steps.
   CONFIGURE_COMMAND touch /tmp/foo
   BUILD_COMMAND touch /tmp/foo
   INSTALL_COMMAND touch /tmp/foo
   INSTALL_DIR ${CMAKE_BINARY_DIR}/third_party/parquet-format
   UPDATE_COMMAND ""
)
# According to docs, ExternalProject_Get_Property will retrieve a
# property from the external project defined above, but it always sets
# a variable that is the same name as the property.
ExternalProject_Get_Property(parquet-format install_dir)
INCLUDE_DIRECTORIES (${install_dir})

ADD_LIBRARY(parquet-thrift STATIC
	${CMAKE_BINARY_DIR}/third_party/parquet-format/parquet_constants.cpp ${CMAKE_BINARY_DIR}/third_party/parquet-format/parquet_types.cpp)

ADD_DEPENDENCIES(parquet-thrift parquet-format)

ExternalProject_Get_Property(parquet-format source_dir)

# The custom command that actually runs thrift to generate the {cc,h}
# files that we need.  The ADD_LIBRARY above depends on the outputs of
# this command.
ADD_CUSTOM_COMMAND(OUTPUT
    ${CMAKE_BINARY_DIR}/third_party/parquet-format/parquet_constants.cpp ${CMAKE_BINARY_DIR}/third_party/parquet-format/parquet_constants.h ${CMAKE_BINARY_DIR}/third_party/parquet-format/parquet_types.cpp ${CMAKE_BINARY_DIR}/third_party/parquet-format/parquet_types.h
    COMMAND mkdir -p ${CMAKE_BINARY_DIR}/third_party/parquet-format
    COMMAND thrift -gen cpp -out ${CMAKE_BINARY_DIR}/third_party/parquet-format ${source_dir}/src/thrift/parquet.thrift
    DEPENDS ${source_dir}/src/thrift/parquet.thrift)
