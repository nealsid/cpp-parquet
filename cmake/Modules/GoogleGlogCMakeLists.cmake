ExternalProject_Add(google-glog
   PREFIX /tmp/google-glog
   SVN_REPOSITORY http://google-glog.googlecode.com/svn/trunk/
   SVN_REVISION -r 142
   CONFIGURE_COMMAND <SOURCE_DIR>/configure --prefix=<INSTALL_DIR>
   BUILD_COMMAND make
   INSTALL_COMMAND make install
   INSTALL_DIR ${CMAKE_BINARY_DIR}/third_party/google-glog
   # Update command has to be set to "", otherwise CMake will refetch
   # & rebuild every time.
   UPDATE_COMMAND ""
)

ExternalProject_Get_Property(google-glog install_dir)
INCLUDE_DIRECTORIES (${install_dir}/include)

ADD_LIBRARY(glog STATIC IMPORTED)
SET_PROPERTY(TARGET glog PROPERTY IMPORTED_LOCATION ${CMAKE_BINARY_DIR}/third_party/google-glog/lib/libglog.a)
ADD_DEPENDENCIES(glog google-glog)
