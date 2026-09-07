/**
 * C++ Consumer - reads data from shared memory queue written by Python producer
 *
 * Usage: cpp_consumer <queue_name> <num_items> <element_size> <output_file>
 */

// // CRITICAL: Force ANSI mode for Python interoperability
// // Python's mmap uses CreateFileMappingA, not CreateFileMappingW
// #ifdef _WIN32
// // Must be BEFORE any Windows headers
// #undef UNICODE
// #undef _UNICODE
// #ifndef _MBCS
// #define _MBCS 1
// #endif
// #endif

#include <limits>
#include <slick/queue.hpp>
#include <iostream>
#include <fstream>
#include <cstring>
#include <cstdint>
#include <chrono>
#include <thread>
#include <array>

// // Compile-time verification that we're in ANSI mode
// #ifdef _WIN32
// #ifdef UNICODE
// #error "UNICODE is defined! This will break Python interop. Check build settings."
// #endif
// #ifdef _UNICODE
// #error "_UNICODE is defined! This will break Python interop. Check build settings."
// #endif
// #ifndef _MBCS
// #error "_MBCS is not defined! ANSI mode not enabled."
// #endif
// #endif

int main(int argc, char* argv[]) {
    if (argc != 5) {
        std::cerr << "Usage: " << argv[0] << " <queue_name> <num_items> <element_size> <output_file>\n";
        return 1;
    }

    const char* queue_name = argv[1];
    int num_items = std::atoi(argv[2]);
    int element_size = std::atoi(argv[3]);
    const char* output_file = argv[4];

    std::cout << "C++ Consumer starting...\n";
    std::cout << "Queue: " << queue_name << "\n";
    std::cout << "Expected items: " << num_items << "\n";
    std::cout << "Element size: " << element_size << "\n";

    // Open existing queue (created by Python)
    // Note: C++ SlickQueue constructor opens existing queue when only name is provided
    // Use array type to match Python's element_size
    using Element = std::array<uint8_t, 32>;
    try {
        slick::queue<Element> queue(queue_name);

        std::cout << "Queue opened successfully!\n";
        std::cout << "  Queue size: " << queue.size() << "\n";

        std::ofstream out(output_file);
        if (!out) {
            std::cerr << "Failed to open output file: " << output_file << "\n";
            return 1;
        }

        uint64_t read_index = 0;
        int consumed = 0;
        int attempts = 0;
        const int MAX_ATTEMPTS = 10000;  // Avoid infinite loop

        std::cout << "Starting read loop, expecting " << num_items << " items...\n";
        std::cout.flush();

        // Consume items
        while (consumed < num_items && attempts < MAX_ATTEMPTS) {
            attempts++;

            // C++ read() takes read_index by reference and updates it
            uint64_t prev_read_index = read_index;
            auto [data, size] = queue.read(read_index);

            // Debug: Print first few attempts
            // if (attempts <= 5) {
            //     std::cout << "Attempt " << attempts << ": read_index=" << prev_read_index
            //               << " -> " << read_index << ", data="
            //               << (data ? "YES " : "NULL ") << ", size=" << size << "\n";
            // }

            if (data != nullptr) {
                // Parse data: [worker_id (4 bytes), item_num (4 bytes)]
                // data is now Element* (std::array<uint8_t, 32>*), access via data->data()
                uint32_t worker_id, item_num;
                std::memcpy(&worker_id, data->data(), sizeof(worker_id));
                std::memcpy(&item_num, data->data() + 4, sizeof(item_num));

                // Write to output file
                out << worker_id << " " << item_num << "\n";

                consumed++;

                if (consumed % 100 == 0) {
                    std::cout << "Progress: " << consumed << "/" << num_items << "\n";
                }
            } else {
                // No data available, small sleep
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }

        out.close();

        if (consumed == num_items) {
            std::cout << "C++ Consumer completed: " << consumed << " items read\n";
            return 0;
        } else {
            std::cerr << "C++ Consumer timeout: only read " << consumed << "/" << num_items << " items\n";
            return 1;
        }

    } catch (const std::exception& e) {
        std::cerr << "C++ Consumer error: " << e.what() << "\n";
        return 1;
    }
}
