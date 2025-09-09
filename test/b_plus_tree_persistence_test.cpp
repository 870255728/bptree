// test/b_plus_tree_persistence_test.cpp

#include "gtest/gtest.h"
#include "b_plus_tree.h" // 包含你的主头文件
#include <string>
#include <vector>
#include <algorithm>
#include <cstdio> // For remove()
#include <random>

namespace bptree {

// --- 测试夹具 (Test Fixture) ---
// 我们为持久化测试创建一个单独的夹具
    class BPlusTreePersistenceTest : public ::testing::Test {
    protected:
        void SetUp() override {
            // 定义一个用于本次测试的数据库文件名
            db_file_name_ = "persistence_test.db";
            // 在每个测试用例开始前，确保清理掉旧的数据库文件
            remove(db_file_name_.c_str());

        }

        void TearDown() override {
            // 在每个测试用-例结束后，清理数据库文件
            remove(db_file_name_.c_str());
        }

        std::string db_file_name_;
    };

    TEST_F(BPlusTreePersistenceTest, DebugMultipleSplits) {
        std::cout << "=== 开始调试MultipleSplits测试 ===" << std::endl;

        BPlusTree<int, int> tree(db_file_name_, 4, 4);
        std::cout << "B+树创建成功，文件: " << db_file_name_ << std::endl;

        // 逐个插入元素并添加调试信息
        for (int i = 1; i <= 15; ++i) {  // 增加到15个元素来观察问题
            std::cout << "\n--- 插入第 " << i << " 个元素 (key=" << i << ", value=" << i * 10 << ") ---" << std::endl;

            try {
                bptree::Transaction txn;
                bool result = tree.Insert(i, i * 10, &txn);
                std::cout << "插入结果: " << (result ? "成功" : "失败") << std::endl;

                if (!result) {
                    std::cout << "插入失败，停止测试" << std::endl;
                    break;
                }

                // 验证刚插入的元素
                int value;
                bool found = tree.Get_Value(i, &value);
                std::cout << "验证刚插入的元素: " << (found ? "找到" : "未找到")
                          << ", 值: " << value << std::endl;

                if (!found) {
                    std::cout << "ERROR: 刚插入的元素无法找到！" << std::endl;
                    break;
                }

            } catch (const std::exception &e) {
                std::cout << "插入过程中发生异常: " << e.what() << std::endl;
                break;
            } catch (...) {
                std::cout << "插入过程中发生未知异常" << std::endl;
                break;
            }
        }

        std::cout << "\n=== 开始验证所有插入的元素 ===" << std::endl;

        // 验证所有插入的元素
        for (int i = 1; i <= 10; ++i) {
            std::cout << "验证元素 " << i << ": ";
            int value;
            bool found = tree.Get_Value(i, &value);
            if (found) {
                std::cout << "找到，值=" << value << std::endl;
                EXPECT_EQ(value, i * 10);
            } else {
                std::cout << "未找到！" << std::endl;
                EXPECT_TRUE(false) << "元素 " << i << " 应该存在但未找到";
            }
        }

        std::cout << "=== 测试完成 ===" << std::endl;
    }

// 测试更小的节点大小，更容易触发分裂
    TEST_F(BPlusTreePersistenceTest, DebugMultipleSplitsSmallNode) {
        std::cout << "=== 开始调试小节点MultipleSplits测试 ===" << std::endl;

        BPlusTree<int, int> tree(db_file_name_, 3, 3);  // 更小的节点大小
        std::cout << "B+树创建成功，节点大小: 3" << std::endl;

        // 逐个插入元素并添加调试信息
        for (int i = 1; i <= 10; ++i) {
            std::cout << "\n--- 插入第 " << i << " 个元素 (key=" << i << ", value=" << i * 10 << ") ---" << std::endl;

            try {
                bptree::Transaction txn;
                bool result = tree.Insert(i, i * 10, &txn);
                std::cout << "插入结果: " << (result ? "成功" : "失败") << std::endl;

                if (!result) {
                    std::cout << "插入失败，停止测试" << std::endl;
                    break;
                }

                // 验证刚插入的元素
                int value;
                bool found = tree.Get_Value(i, &value);
                std::cout << "验证刚插入的元素: " << (found ? "找到" : "未找到")
                          << ", 值: " << value << std::endl;

                if (!found) {
                    std::cout << "ERROR: 刚插入的元素无法找到！" << std::endl;
                    break;
                }

            } catch (const std::exception &e) {
                std::cout << "插入过程中发生异常: " << e.what() << std::endl;
                break;
            } catch (...) {
                std::cout << "插入过程中发生未知异常" << std::endl;
                break;
            }
        }

        std::cout << "\n=== 开始验证所有插入的元素 ===" << std::endl;

        // 验证所有插入的元素
        for (int i = 1; i <= 10; ++i) {
            std::cout << "验证元素 " << i << ": ";
            int value;
            bool found = tree.Get_Value(i, &value);
            if (found) {
                std::cout << "找到，值=" << value << std::endl;
                EXPECT_EQ(value, i * 10);
            } else {
                std::cout << "未找到！" << std::endl;
                EXPECT_TRUE(false) << "元素 " << i << " 应该存在但未找到";
            }
        }

        std::cout << "=== 测试完成 ===" << std::endl;
    }


// 测试基本的分裂功能
    TEST_F(BPlusTreePersistenceTest, BasicSplit) {

        BPlusTree<int, int> tree(db_file_name_, 4, 4);  // 叶子节点最大大小为4

        // 插入5个元素，第5个元素应该触发分裂
        {
            bptree::Transaction txn;
            EXPECT_TRUE(tree.Insert(1, 10, &txn));
            EXPECT_TRUE(tree.Insert(2, 20, &txn));
            EXPECT_TRUE(tree.Insert(3, 30, &txn));
            EXPECT_TRUE(tree.Insert(4, 40, &txn));
            EXPECT_TRUE(tree.Insert(5, 50, &txn));
        }

        // 验证所有元素都能被找到
        int value;
        EXPECT_TRUE(tree.Get_Value(1, &value));
        EXPECT_EQ(value, 10);

        EXPECT_TRUE(tree.Get_Value(2, &value));
        EXPECT_EQ(value, 20);

        EXPECT_TRUE(tree.Get_Value(3, &value));
        EXPECT_EQ(value, 30);

        EXPECT_TRUE(tree.Get_Value(4, &value));
        EXPECT_EQ(value, 40);

        EXPECT_TRUE(tree.Get_Value(5, &value));
        EXPECT_EQ(value, 50);
    }

// 测试1：简单创建、插入并重新打开
    TEST_F(BPlusTreePersistenceTest, SimpleCreateInsertAndReopen) {
        // --- 第一阶段：创建和插入 ---
        {
            // 创建一个新的B+Tree实例，它会自动创建数据库文件

            BPlusTree<int, int> tree(db_file_name_, 4, 4);

            // 插入一些数据，确保会发生至少一次分裂
            {
                bptree::Transaction txn;
                tree.Insert(10, 100, &txn);
                tree.Insert(20, 200, &txn);
                tree.Insert(30, 300, &txn);
                tree.Insert(15, 150, &txn); // 触发分裂
            }

            // 在这个作用域结束时，tree的析构函数会被调用。
            // 我们的B+Tree析构函数设计为会将根页面ID写回元数据页，
            // 并且其成员bpm_的析构函数会Flush所有脏页。
        }

        // --- 第二阶段：重新打开并验证 ---
        {
            // 使用同一个文件名，重新创建一个B+Tree实例
            BPlusTree<int, int> tree(db_file_name_, 4, 4);

            // 断言1：树不应该是空的
            ASSERT_FALSE(tree.Is_Empty());

            // 断言2：验证之前插入的所有数据都依然存在
            int value;
            EXPECT_TRUE(tree.Get_Value(10, &value));
            EXPECT_EQ(value, 100);

            EXPECT_TRUE(tree.Get_Value(15, &value));
            EXPECT_EQ(value, 150);

            EXPECT_TRUE(tree.Get_Value(20, &value));
            EXPECT_EQ(value, 200);

            EXPECT_TRUE(tree.Get_Value(30, &value));
            EXPECT_EQ(value, 300);

            // 断言3：验证一个不存在的键
            EXPECT_FALSE(tree.Get_Value(99, &value));
        }
    }


// 测试2：经过复杂操作（插入和删除）后的持久化
    TEST_F(BPlusTreePersistenceTest, ComplexOperationsAndReopen) {
        std::vector<int> initial_keys;
        for (int i = 0; i < 50; ++i) {
            initial_keys.push_back(i);
        }

        // --- 第一阶段：创建并执行大量插入和部分删除 ---
        {
            BPlusTree<int, int> tree(db_file_name_, 8, 8);

            // 随机插入50个键
            std::random_device rd;
            std::mt19937 g(rd());
            std::shuffle(initial_keys.begin(), initial_keys.end(), g);
            for (int key: initial_keys) {
                bptree::Transaction txn;
                tree.Insert(key, key * 10, &txn);
            }

            // 删除其中的偶数键
            for (int i = 0; i < 50; i += 2) {
                bptree::Transaction txn;
                tree.Remove(i, &txn);
            }

            // 此时，树中应该只剩下奇数键
        } // tree 析构，所有更改应被持久化

        // --- 第二阶段：重新打开并进行全面验证 ---
        {
            BPlusTree<int, int> tree(db_file_name_, 8, 8);

            ASSERT_FALSE(tree.Is_Empty());

            // 断言1：所有奇数键都应该存在
            int value;
            for (int i = 1; i < 50; i += 2) {
                EXPECT_TRUE(tree.Get_Value(i, &value)) << "Failed to find key: " << i;
                EXPECT_EQ(value, i * 10) << "Wrong value for key: " << i;
            }

            // 断言2：所有偶数键都应该不存在
            for (int i = 0; i < 50; i += 2) {
                EXPECT_FALSE(tree.Get_Value(i, &value)) << "Found a deleted key: " << i;
            }

            // 断言3：使用迭代器验证所有剩余元素的顺序和数量
            std::vector<int> expected_keys;
            for (int i = 1; i < 50; i += 2) {
                expected_keys.push_back(i);
            }

            int count = 0;
            for (const auto &pair: tree) {
                ASSERT_LT(count, expected_keys.size());
                EXPECT_EQ(pair.first, expected_keys[count]);
                count++;
            }
            EXPECT_EQ(count, expected_keys.size());
        }
    }

} // namespace bptree