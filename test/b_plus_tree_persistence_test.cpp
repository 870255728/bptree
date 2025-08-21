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


// 测试1：简单创建、插入并重新打开
    TEST_F(BPlusTreePersistenceTest, SimpleCreateInsertAndReopen) {
        // --- 第一阶段：创建和插入 ---
        {
            // 创建一个新的B+Tree实例，它会自动创建数据库文件
            BPlusTree<int, int> tree(db_file_name_, 4, 4);

            // 插入一些数据，确保会发生至少一次分裂
            tree.Insert(10, 100);
            tree.Insert(20, 200);
            tree.Insert(30, 300);
            tree.Insert(15, 150); // 触发分裂

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
            for (int key : initial_keys) {
                tree.Insert(key, key * 10);
            }

            // 删除其中的偶数键
            for (int i = 0; i < 50; i += 2) {
                tree.Remove(i);
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
            for (const auto& pair : tree) {
                ASSERT_LT(count, expected_keys.size());
                EXPECT_EQ(pair.first, expected_keys[count]);
                count++;
            }
            EXPECT_EQ(count, expected_keys.size());
        }
    }

} // namespace bptree