// test/b_plus_tree_concurrency_test.cpp

#include "gtest/gtest.h"
#include "b_plus_tree.h"
#include <thread>
#include <vector>
#include <atomic>
#include <random>

namespace bptree {
    TEST(BPlusTreeConcurrencyTest, ConcurrentReadTest) {
        // --- 1. ARRANGE: Set up a well-populated B+Tree ---

        // Use relatively small node sizes to create a deeper tree, which better
        // tests the latch crabbing mechanism across multiple levels.
        const int leaf_max_size = 16;
        const int internal_max_size = 16;
        BPlusTree<int, int> tree(leaf_max_size, internal_max_size);

        const int num_keys = 2000;
        std::vector<int> keys;
        for (int i = 0; i < num_keys; ++i) {
            keys.push_back(i);
            Transaction txn;
            tree.Insert(i, i * 10, &txn); // Insert keys 0-1999, with value = key * 10
        }

        // --- 2. ACT: Spawn multiple threads to read concurrently ---

        const int num_threads = 8;
        const int reads_per_thread = 1000;
        std::vector<std::thread> threads;
        std::atomic<bool> any_thread_failed(false); // Shared flag to signal failure

        for (int i = 0; i < num_threads; ++i) {
            threads.emplace_back([&]() {
                // Each thread gets its own random number generator to avoid contention
                std::random_device rd;
                std::mt19937 gen(rd());
                // We will search for keys in a range larger than what was inserted
                // to test both successful and unsuccessful lookups.
                std::uniform_int_distribution<> distrib(0, num_keys * 2);

                for (int j = 0; j < reads_per_thread; ++j) {
                    int key_to_find = distrib(gen);
                    int result_value = -1;

                    // --- Execute the function under test ---
                    bool found = tree.Get_Value(key_to_find, &result_value);

                    // --- Verify the result ---
                    if (key_to_find >= 0 && key_to_find < num_keys) {
                        // This key should exist.
                        if (!found) {
                            // Error: An existing key was not found.
                            std::cerr << "Thread Error: Key " << key_to_find << " was not found!" << std::endl;
                            any_thread_failed = true;
                            break; // Stop this thread's execution
                        }
                        if (result_value != key_to_find * 10) {
                            // Error: The value for an existing key is incorrect.
                            std::cerr << "Thread Error: Key " << key_to_find << " has wrong value " << result_value << std::endl;
                            any_thread_failed = true;
                            break;
                        }
                    } else {
                        // This key should NOT exist.
                        if (found) {
                            // Error: A non-existent key was found.
                            std::cerr << "Thread Error: Non-existent key " << key_to_find << " was found!" << std::endl;
                            any_thread_failed = true;
                            break;
                        }
                    }
                }
            });
        }

        // Wait for all reader threads to complete their work
        for (auto &t : threads) {
            t.join();
        }

        // --- 3. ASSERT: Check if any thread reported an error ---
        EXPECT_FALSE(any_thread_failed.load())
                            << "One or more threads failed during concurrent reads. Check console output for details.";
    }

} // namespace bptree