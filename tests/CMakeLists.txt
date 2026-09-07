include(FetchContent)

# Fetch slick-queue from GitHub
set(BUILD_SLICK_QUEUE_TESTS OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
    slick-queue
    GIT_REPOSITORY https://github.com/SlickQuant/slick-queue.git
    GIT_TAG main
)
FetchContent_MakeAvailable(slick-queue)

set(SLICK_SHM_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(SLICK_SHM_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(SLICK_SHM_INSTALL OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
    slick-shm
    GIT_REPOSITORY https://github.com/SlickQuant/slick-shm.git
    GIT_TAG main
)
FetchContent_MakeAvailable(slick-shm)

# Find Python
find_package(Python3 COMPONENTS Interpreter Development REQUIRED)

# Create interop test executables
add_executable(cpp_producer cpp_producer.cpp)
target_link_libraries(cpp_producer PRIVATE slick::queue)

add_executable(cpp_consumer cpp_consumer.cpp)
target_link_libraries(cpp_consumer PRIVATE slick::queue)

add_executable(cpp_work_stealing_consumer cpp_work_stealing_consumer.cpp)
target_link_libraries(cpp_work_stealing_consumer PRIVATE slick::queue slick::shm)

add_executable(cpp_multi_producer cpp_multi_producer.cpp)
target_link_libraries(cpp_multi_producer PRIVATE slick::queue)

add_executable(cpp_read_last_tester cpp_read_last_tester.cpp)
target_link_libraries(cpp_read_last_tester PRIVATE slick::queue)

# Platform-specific linking and compilation flags
if(UNIX AND NOT APPLE)
    # Link rt library on Linux (not needed on macOS)
    target_link_libraries(cpp_producer PRIVATE pthread rt)
    target_link_libraries(cpp_consumer PRIVATE pthread rt)
    target_link_libraries(cpp_work_stealing_consumer PRIVATE pthread rt)
    target_link_libraries(cpp_multi_producer PRIVATE pthread rt)
    target_link_libraries(cpp_read_last_tester PRIVATE pthread rt)
elseif(APPLE)
    # macOS only needs pthread
    target_link_libraries(cpp_producer PRIVATE pthread)
    target_link_libraries(cpp_consumer PRIVATE pthread)
    target_link_libraries(cpp_work_stealing_consumer PRIVATE pthread)
    target_link_libraries(cpp_multi_producer PRIVATE pthread)
    target_link_libraries(cpp_read_last_tester PRIVATE pthread)
endif()

# Add Python interop tests
add_test(
    NAME atomic_ops_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_atomic_ops.py
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_test(
    NAME atomic_cursor_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_atomic_cursor.py
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_test(
    NAME local_mode_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_local_mode.py
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_test(
    NAME shm_mode_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_queue.py
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_test(
    NAME multi_producer_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_multi_producer.py
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

# Python/C++ interop tests
add_test(
    NAME python_producer_cpp_consumer
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_interop.py --test python_producer_cpp_consumer
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_test(
    NAME cpp_producer_python_consumer
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_interop.py --test cpp_producer_python_consumer
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_test(
    NAME multi_producer_interop
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_interop.py --test multi_producer_interop
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_test(
    NAME read_last_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_read_last.py
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_test(
    NAME read_last_interop_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_read_last_interop.py
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_test(
    NAME modern_format_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_modern_format.py
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_test(
    NAME loss_count_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_loss_count.py
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_test(
    NAME traits_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_traits.py
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_test(
    NAME reset_detection_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_reset_detection.py
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_test(
    NAME wrap_invariants_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_wrap_invariants.py
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

# Set timeouts for all tests to prevent CI hangs
set_tests_properties(
    atomic_ops_test
    atomic_cursor_test
    local_mode_test
    shm_mode_test
    multi_producer_test
    python_producer_cpp_consumer
    cpp_producer_python_consumer
    multi_producer_interop
    read_last_test
    read_last_interop_test
    modern_format_test
    loss_count_test
    traits_test
    reset_detection_test
    wrap_invariants_test
    PROPERTIES TIMEOUT 120
)
