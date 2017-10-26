cmake_minimum_required(VERSION 2.8.3)
project(libmongocxx_ros)

find_package(catkin REQUIRED COMPONENTS)

include(ProcessorCount)
ProcessorCount(PROCESSOR_COUNT)

include(ExternalProject)


catkin_package(
	LIBRARIES mongoclient
	INCLUDE_DIRS include
)

ExternalProject_add(mongocxx
	GIT_REPOSITORY https://github.com/mongodb/mongo-cxx-driver.git
	GIT_TAG 26compat
	PREFIX ${CMAKE_CURRENT_BINARY_DIR}
	BUILD_IN_SOURCE 1
	UPDATE_COMMAND ""
	CONFIGURE_COMMAND ""
	SOURCE_DIR ${CMAKE_CURRENT_BINARY_DIR}/mongo_cxx
	BUILD_COMMAND scons --use-system-boost -j ${PROCESSOR_COUNT} --full --ssl --sharedclient --disable-warnings-as-errors --prefix=${CATKIN_DEVEL_PREFIX}
	INSTALL_COMMAND ""
	LOG_DOWNLOAD true
	LOG_BUILD true
	LOG_INSTALL true
)

add_library(mongoclient_imp SHARED IMPORTED)
set_target_properties(mongoclient_imp PROPERTIES IMPORTED_LOCATION ${CATKIN_DEVEL_PREFIX}/lib/libmongoclient.so)

add_library(mongoclient)
add_custom_command( TARGET mongoclient POST_BUILD COMMAND cp ${CATKIN_DEVEL_PREFIX}/lib/libmongoclient.so ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/ )
set_target_properties(mongoclient PROPERTIES LINKER_LANGUAGE CXX )


add_dependencies(mongoclient_imp mongocxx)
add_dependencies(mongoclient mongoclient_imp)

include_directories(
  ${CATKIN_DEVEL_PREFIX}/include
  ${catkin_INCLUDE_DIRS}
)

install(TARGETS mongoclient
   ARCHIVE DESTINATION ${CATKIN_PACKAGE_LIB_DESTINATION}
   LIBRARY DESTINATION ${CATKIN_PACKAGE_LIB_DESTINATION}
   RUNTIME DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
)

install(DIRECTORY ${CATKIN_DEVEL_PREFIX}/include/
  DESTINATION ${CMAKE_INSTALL_PREFIX}/include
  FILES_MATCHING PATTERN "*.h"
  PATTERN ".svn" EXCLUDE
)
