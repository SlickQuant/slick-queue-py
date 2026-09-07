/**
 * C++ Consumer - reads data from shared memory queue written by Python producer
 *
 * Usage: cpp_consumer <queue_name> <num_items> <element_size> <output_file>
 */

#include <limits>
#include <slick/queue.hpp>
#include <iostream>
#include <fstream>
#include <cstring>
#include <cstdint>
#include <chrono>
#include <thread>
#include <array>
#include <slick/shm/shared_memory.hpp>
#include <filesystem>
#include <random>

namespace fs = std::filesystem;

int main(int argc, char* argv[]) {
    if (argc != 6) {
        std::cerr << "Usage: " << argv[0] << " <queue_name> <num_items> <element_size> <atomic_cursor_name> <output_file>\n";
        return 1;
    }

    const char* queue_name = argv[1];
    int num_items = std::atoi(argv[2]);
    int element_size = std::atoi(argv[3]);
    const char* cursor_name = argv[4];
    const char* output_file = argv[5];
    
    std::cout << "C++ Work StealingConsumer starting...\n";
    std::cout << "Queue: " << queue_name << "\n";
    std::cout << "Cursor shm: " << cursor_name << "\n";
    std::cout << "Expected items: " << num_items << "\n";
    std::cout << "Element size: " << element_size << "\n";

    std::random_device rd;
    std::mt19937 engine(rd());

    const double lower_bound = 1;
    const double upper_bound = 5;

    // This produces random numbers in the range [lower_bound, upper_bound)
    std::uniform_real_distribution<double> dist(lower_bound, upper_bound);

    // Open existing queue (created by Python)
    // Note: C++ SlickQueue constructor opens existing queue when only name is provided
    // Use array type to match Python's element_size
    using Element = std::array<uint8_t, 32>;
    try {
        slick::queue<Element> queue(queue_name);

        std::atomic<uint64_t>* atomic_cursor;
        slick::shm::shared_memory shm(cursor_name, sizeof(std::atomic<uint64_t>), slick::shm::create_only, slick::shm::access_mode::read_write, std::nothrow);
        if (shm.is_valid()) {
            // Initialize cursor
            atomic_cursor = new (shm.data())std::atomic<uint64_t>();
        }
        else {
            if (shm.last_error() == slick::shm::errc::already_exists) {
                shm = slick::shm::shared_memory(cursor_name, slick::shm::open_existing);
            }
            else {
                std::cerr << "Failed to create or open atomic cursor shared memory: " << cursor_name << "\n";
                return 1;
            }
            atomic_cursor = reinterpret_cast<std::atomic<uint64_t>*>(shm.data());
        }

        std::cout << "Queue opened successfully!\n";
        std::cout << "  Queue size: " << queue.size() << "\n";

        std::ofstream out(output_file);
        if (!out) {
            std::cerr << "Failed to open output file: " << output_file << "\n";
            return 1;
        }

        while (!fs::exists("ready")) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        std::atomic<uint64_t>& read_index = *atomic_cursor;
        int consumed = 0;
        const int MAX_ATTEMPTS = 1000;  // Avoid infinite loop

        std::cout << "Starting read loop, expecting " << num_items << " items...\n";
        std::cout.flush();


        // Consume items
        while (read_index.load(std::memory_order_relaxed) < num_items) {
            // C++ read() takes read_index by reference and updates it
            // uint64_t prev_read_index = read_index.load(std::memory_order_relaxed);
            auto [data, size] = queue.read(read_index);

            // Debug: Print first few attempts
            // if (attempts <= 5) {
                // std::cout << "Attempt " << attempts << ": read_index=" << prev_read_index
                //           << " -> " << read_index << ", data="
                //           << (data ? "YES " : "NULL ") << ", size=" << size << "\n";
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

                std::this_thread::sleep_for(std::chrono::milliseconds((int)dist(engine)));
            } else {
                std::this_thread::sleep_for(std::chrono::microseconds(10));
            }
        }

        out.close();

        std::cout << "C++ Consumer completed " << consumed << " items";

        return 0;

    } catch (const std::exception& e) {
        std::cerr << "C++ Consumer error: " << e.what() << "\n";
        return 1;
    }
}
