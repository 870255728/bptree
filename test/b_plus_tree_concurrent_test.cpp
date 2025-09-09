// test/b_plus_tree_concurrent_test.cpp

#include "gtest/gtest.h"
#include "b_plus_tree.h"
#include "transaction.h"
#include <string>
#include <vector>
#include <thread>
#include <random>
#include <chrono>
#include <atomic>
#include <mutex>

namespace bptree {

// --- 测试夹具 (Test Fixture) ---
    class BPlusTreeConcurrentTest : public ::testing::Test {
    protected:
        void SetUp() override {
            // 定义一个用于本次测试的数据库文件名
            db_file_name_ = "concurrent_test.db";
            // 在每个测试用例开始前，确保清理掉旧的数据库文件
            remove(db_file_name_.c_str());
        }

        void TearDown() override {
            // 在每个测试用例结束后，清理数据库文件
            remove(db_file_name_.c_str());
        }

        std::string db_file_name_;
    };

// 测试1：基本并发插入测试
    TEST_F(BPlusTreeConcurrentTest, BasicConcurrentInsert) {
        BPlusTree<int, int> tree(db_file_name_, 4, 4);
        std::atomic<int> success_count{0};
        std::atomic<int> duplicate_count{0};

        // 创建多个线程进行并发插入
        std::vector<std::thread> threads;
        const int num_threads = 20;
        const int inserts_per_thread = 100;

        for (int i = 0; i < num_threads; ++i) {
            threads.emplace_back([&tree, &success_count, &duplicate_count, i, inserts_per_thread]() {
                Transaction transaction;
                for (int j = 0; j < inserts_per_thread; ++j) {
                    int key = i * inserts_per_thread + j;
                    if (tree.Insert(key, key * 10, &transaction)) {
                        success_count++;
                    } else {
                        duplicate_count++;
                    }
                }
            });
        }

        // 等待所有线程完成
        for (auto &thread: threads) {
            thread.join();
        }

        // 验证结果
        EXPECT_EQ(success_count.load(), num_threads * inserts_per_thread);
        EXPECT_EQ(duplicate_count.load(), 0);

        // 验证所有插入的数据都存在
        for (int i = 0; i < num_threads * inserts_per_thread; ++i) {
            int value;
            EXPECT_TRUE(tree.Get_Value(i, &value));
            EXPECT_EQ(value, i * 10);
        }
    }

// 测试2：并发插入和查找测试
    TEST_F(BPlusTreeConcurrentTest, ConcurrentInsertAndSearch) {
        BPlusTree<int, int> tree(db_file_name_, 4, 4);
        std::atomic<int> insert_success{0};
        std::atomic<int> search_success{0};
        std::atomic<int> search_fail{0};

        // 先插入一些初始数据
        for (int i = 0; i < 50; ++i) {
            Transaction txn;
            tree.Insert(i, i * 10, &txn);
        }

        // 创建插入线程
        std::thread insert_thread([&tree, &insert_success]() {
            Transaction transaction;
            for (int i = 50; i < 150; ++i) {
                if (tree.Insert(i, i * 10, &transaction)) {
                    insert_success++;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        });

        // 创建查找线程
        std::thread search_thread([&tree, &search_success, &search_fail]() {
            for (int i = 0; i < 200; ++i) {
                int value;
                if (tree.Get_Value(i, &value)) {
                    search_success++;
                    EXPECT_EQ(value, i * 10);
                } else {
                    search_fail++;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        });

        // 等待线程完成
        insert_thread.join();
        search_thread.join();

        // 验证结果
        EXPECT_GT(insert_success.load(), 0);
        EXPECT_GT(search_success.load(), 0);

        // 验证所有插入的数据都存在
        for (int i = 0; i < 150; ++i) {
            int value;
            EXPECT_TRUE(tree.Get_Value(i, &value));
            EXPECT_EQ(value, i * 10);
        }
    }

// 测试3：并发插入和删除测试
    TEST_F(BPlusTreeConcurrentTest, ConcurrentInsertAndDelete) {

        //第一阶段创建树并并发插入和删除
        {
            BPlusTree<int, int> tree(db_file_name_, 4, 4);
            std::atomic<int> insert_success{0};
            std::atomic<int> delete_success{0};
            Transaction transaction;

            // 先插入一些初始数据
            for (int i = 0; i < 100; ++i) {
                tree.Insert(i, i * 10, &transaction);
            }

            // 创建插入线程
            std::thread insert_thread([&tree, &insert_success]() {
                Transaction transaction;
                for (int i = 100; i < 200; ++i) {
                    if (tree.Insert(i, i * 10, &transaction)) {
                        insert_success++;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
            });

            // 创建删除线程
            std::thread delete_thread([&tree, &delete_success]() {
                Transaction transaction;
                for (int i = 0; i < 50; ++i) {
                    tree.Remove(i, &transaction);
                    delete_success++;
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
            });

            // 等待线程完成
            insert_thread.join();
            delete_thread.join();

            // 验证结果
            EXPECT_GT(insert_success.load(), 0);
            EXPECT_GT(delete_success.load(), 0);
        }
        //第二阶段重新打开，验证持久化
        {
            BPlusTree<int, int> tree(db_file_name_, 4, 4);
            // 验证删除的数据不存在，未删除的数据存在
            for (int i = 0; i < 50; ++i) {
                int value;
                EXPECT_FALSE(tree.Get_Value(i, &value));
            }

            for (int i = 50; i < 200; ++i) {
                int value;
                EXPECT_TRUE(tree.Get_Value(i, &value));
                EXPECT_EQ(value, i * 10);
            }

        }
    }

// 测试4：多线程随机操作测试
    TEST_F(BPlusTreeConcurrentTest, RandomOperations) {
        BPlusTree<int, int> tree(db_file_name_, 4, 4);
        std::atomic<int> total_operations{0};
        std::mutex tree_mutex;

        // 创建多个线程进行随机操作
        std::vector<std::thread> threads;
        const int num_threads = 4;
        const int operations_per_thread = 100;

        for (int i = 0; i < num_threads; ++i) {
            threads.emplace_back([&tree, &total_operations, &tree_mutex, i, operations_per_thread]() {
                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_int_distribution<> op_dist(0, 2); // 0=insert, 1=search, 2=delete
                std::uniform_int_distribution<> key_dist(0, 99);

                Transaction transaction;

                for (int j = 0; j < operations_per_thread; ++j) {
                    int op = op_dist(gen);
                    int key = key_dist(gen);

                    switch (op) {
                        case 0: // Insert
                            tree.Insert(key, key * 10, &transaction);
                            break;
                        case 1: // Search
                        {
                            int value;
                            tree.Get_Value(key, &value, &transaction);
                        }
                            break;
                        case 2: // Delete
                            tree.Remove(key, &transaction);
                            break;
                    }

                    total_operations++;
                    std::this_thread::sleep_for(std::chrono::microseconds(100));
                }
            });
        }

        // 等待所有线程完成
        for (auto &thread: threads) {
            thread.join();
        }

        // 验证操作完成
        EXPECT_EQ(total_operations.load(), num_threads * operations_per_thread);

        // 验证树的一致性（没有崩溃）
        EXPECT_FALSE(tree.Is_Empty() || tree.Is_Empty());
    }

// 测试5：高并发压力测试
    TEST_F(BPlusTreeConcurrentTest, HighConcurrencyStressTest) {
        BPlusTree<int, int> tree(db_file_name_, 8, 8);
        std::atomic<int> insert_count{0};
        std::atomic<int> search_count{0};
        std::atomic<int> delete_count{0};

        // 创建更多线程进行高并发测试
        std::vector<std::thread> threads;
        const int num_threads = 8;
        const int operations_per_thread = 200;

        for (int i = 0; i < num_threads; ++i) {
            threads.emplace_back([&tree, &insert_count, &search_count, &delete_count, i, operations_per_thread]() {
                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_int_distribution<> op_dist(0, 2);
                std::uniform_int_distribution<> key_dist(0, 999);

                Transaction transaction;

                for (int j = 0; j < operations_per_thread; ++j) {
                    int op = op_dist(gen);
                    int key = key_dist(gen);

                    switch (op) {
                        case 0: // Insert
                            if (tree.Insert(key, key * 10, &transaction)) {
                                insert_count++;
                            }
                            break;
                        case 1: // Search
                        {
                            int value;
                            if (tree.Get_Value(key, &value, &transaction)) {
                                search_count++;
                            }
                        }
                            break;
                        case 2: // Delete
                            tree.Remove(key, &transaction);
                            delete_count++;
                            break;
                    }
                }
            });
        }

        // 等待所有线程完成
        for (auto &thread: threads) {
            thread.join();
        }

        // 验证操作完成
        EXPECT_GT(insert_count.load(), 0);
        EXPECT_GT(search_count.load(), 0);
        EXPECT_GT(delete_count.load(), 0);

        // 验证树的一致性
        EXPECT_FALSE(tree.Is_Empty() || tree.Is_Empty());
    }

// 测试6：字符串类型的并发测试
//TEST_F(BPlusTreeConcurrentTest, StringKeyConcurrentTest) {
//    BPlusTree<std::string, uint64_t> tree(db_file_name_, 4, 4);
//    std::atomic<int> success_count{0};
//
//    // 创建多个线程进行并发插入
//    std::vector<std::thread> threads;
//    const int num_threads = 4;
//    const int inserts_per_thread = 50;
//
//    for (int i = 0; i < num_threads; ++i) {
//        threads.emplace_back([&tree, &success_count, i, inserts_per_thread]() {
//            Transaction transaction;
//            for (int j = 0; j < inserts_per_thread; ++j) {
//                std::string key = "thread_" + std::to_string(i) + "_key_" + std::to_string(j);
//                uint64_t value = static_cast<uint64_t>(i * inserts_per_thread + j);
//                if (tree.Insert(key, value, &transaction)) {
//                    success_count++;
//                }
//            }
//        });
//    }
//
//    // 等待所有线程完成
//    for (auto& thread : threads) {
//        thread.join();
//    }
//
//    // 验证结果
//    EXPECT_EQ(success_count.load(), num_threads * inserts_per_thread);
//
//    // 验证所有插入的数据都存在
//    for (int i = 0; i < num_threads; ++i) {
//        for (int j = 0; j < inserts_per_thread; ++j) {
//            std::string key = "thread_" + std::to_string(i) + "_key_" + std::to_string(j);
//            uint64_t expected_value = static_cast<uint64_t>(i * inserts_per_thread + j);
//            uint64_t value;
//            EXPECT_TRUE(tree.Get_Value(key, &value));
//            EXPECT_EQ(value, expected_value);
//        }
//    }
//}

} // namespace bptree
