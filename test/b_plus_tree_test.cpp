#include "gtest/gtest.h"
#include "b_plus_tree.h"
#include <vector>
#include <algorithm>
#include <random>
#include <string> // 用于测试不同类型的Key

// =====================================================================
// 测试套件：BPlusTreeInsertionTest
// 目的：验证B+Tree的插入操作和基本的查找功能是否正确，
// 包括空树、简单插入、叶子分裂和内部节点分裂等场景。
// =====================================================================

// 测试1：空树行为
TEST(BPlusTreeInsertionTest, HandlesEmptyTree) {
   // 使用int作为键和值
   bptree::BPlusTree<int, int> tree(3, 3);

   // 断言1：新创建的树应该是空的
   EXPECT_TRUE(tree.Is_Empty());

   // 断言2：在空树中查找任何键都应该失败
   int value;
   EXPECT_FALSE(tree.Get_Value(10, &value));
}

// 测试2：简单的插入和获取
TEST(BPlusTreeInsertionTest, HandlesSimpleInsertAndGet) {
   bptree::BPlusTree<int, int> tree(3, 3);

   // 断言1：成功插入一个新键
   EXPECT_TRUE(tree.Insert(10, 100));
   EXPECT_FALSE(tree.Is_Empty());

   // 断言2：能够查找到刚刚插入的键
   int value;
   EXPECT_TRUE(tree.Get_Value(10, &value));
   EXPECT_EQ(value, 100);

   // 断言3：查找一个不存在的键会失败
   EXPECT_FALSE(tree.Get_Value(20, &value));

   // 断言4：插入一个重复的键会失败 (返回false)
   EXPECT_FALSE(tree.Insert(10, 200));

   // 断言5：重复插入不会覆盖原有的值
   EXPECT_TRUE(tree.Get_Value(10, &value));
   EXPECT_EQ(value, 100);
}

// 测试3：叶子节点分裂
TEST(BPlusTreeInsertionTest, HandlesLeafNodeSplit) {
   // 叶子容量为3，意味着最多2个键。插入第3个键(15)时应触发分裂。
   bptree::BPlusTree<int, int> tree(3, 3);

   tree.Insert(10, 100);
   tree.Insert(20, 200);
   tree.Insert(15, 150); // 触发分裂

   // 验证所有键在分裂后依然存在且值正确
   int value;
   EXPECT_TRUE(tree.Get_Value(10, &value));
   EXPECT_EQ(value, 100);
   EXPECT_TRUE(tree.Get_Value(15, &value));
   EXPECT_EQ(value, 150);
   EXPECT_TRUE(tree.Get_Value(20, &value));
   EXPECT_EQ(value, 200);

   // 此时，树的结构应该变为：
   //       [15] (根，内部节点)
   //      /    \
   // [10] (叶子) [15, 20] (叶子)
}

// 测试4：内部节点分裂（级联分裂）
TEST(BPlusTreeInsertionTest, HandlesInternalNodeSplit) {
   // 内部节点和叶子节点容量都为3（最多2个键）。
   bptree::BPlusTree<int, int> tree(4, 4);

   tree.Insert(10, 100);
   tree.Insert(20, 200);
   tree.Insert(30, 300);

   tree.Insert(15, 150);
   tree.Insert(40, 400);

   tree.Insert(50, 500);
   tree.Insert(35, 350);

   // 验证所有键都存在
   int value;
   EXPECT_TRUE(tree.Get_Value(10, &value));
   EXPECT_TRUE(tree.Get_Value(20, &value));
   EXPECT_TRUE(tree.Get_Value(35, &value));
   EXPECT_TRUE(tree.Get_Value(15, &value));
   EXPECT_TRUE(tree.Get_Value(40, &value));
   EXPECT_TRUE(tree.Get_Value(35, &value));
}

// 测试5：大量随机插入
TEST(BPlusTreeInsertionTest, HandlesLargeNumberOfRandomInsertions) {
   bptree::BPlusTree<int, int> tree(10, 10);
   std::vector<int> keys;
   for (int i = 0; i < 50; ++i) {
       keys.push_back(i);
   }

   // 使用随机顺序插入，能更好地测试各种分裂场景
   std::random_device rd;
   std::mt19937 g(rd());
   std::shuffle(keys.begin(), keys.end(), g);

   for (int key: keys) {
       ASSERT_TRUE(tree.Insert(key, key * 10)); // 使用ASSERT确保插入不会意外失败
   }

   // 验证所有插入的键都能被找到
   int value;
   for (int key: keys) {
       EXPECT_TRUE(tree.Get_Value(key, &value));
       EXPECT_EQ(value, key * 10);
   }
}

// =====================================================================
// 测试套件：BPlusTreeIteratorTest
// 目的：验证迭代器和范围扫描功能的正确性。
// =====================================================================

// 测试1：空树上的迭代器
TEST(BPlusTreeIteratorTest, HandlesEmptyTreeIteration) {
   bptree::BPlusTree<int, int> tree(3, 3);

   // 断言：对于空树，Begin()返回的迭代器应该等于End()返回的迭代器
   EXPECT_EQ(tree.Begin(), tree.End());
}

// 测试2：简单的顺序遍历
TEST(BPlusTreeIteratorTest, HandlesSimpleIteration) {
   // 使用整型键验证顺序遍历
   bptree::BPlusTree<int, int> tree(3, 3);
   tree.Insert(2, 100);
   tree.Insert(3, 200);
   tree.Insert(1, 50);

   std::vector<std::pair<int, int>> expected_result = {
           {1, 50},
           {2, 100},
           {3, 200}
   };

   int i = 0;
   for (const auto &pair: tree) {
       ASSERT_LT(i, expected_result.size());
       EXPECT_EQ(pair.first, expected_result[i].first);
       EXPECT_EQ(pair.second, expected_result[i].second);
       i++;
   }

   EXPECT_EQ(i, expected_result.size());
}

// 测试3：跨越多个分裂节点的遍历
TEST(BPlusTreeIteratorTest, HandlesIterationAcrossNodeSplits) {
   bptree::BPlusTree<int, int> tree(4, 4);
   std::vector<int> keys = {10, 20, 15, 30, 25, 5, 40, 50, 35, 1};
   for (int key: keys) {
       tree.Insert(key, key * 10);
   }

   std::sort(keys.begin(), keys.end());

   // 使用传统的迭代器循环来验证
   auto it = tree.Begin();
   for (int key: keys) {
       ASSERT_NE(it, tree.End());
       // 将 it->first 和 it->second 修改为 (*it).first 和 (*it).second
       EXPECT_EQ((*it).first, key);
       EXPECT_EQ((*it).second, key * 10);
       ++it;
   }

   EXPECT_EQ(it, tree.End());
}

// =====================================================================
// 测试套件：BPlusTreeRemoveTest
// 目的：验证删除操作的正确性，包括简单删除、合并和重分配。
// =====================================================================

// 测试1：在一个未满的叶子节点中进行简单删除
TEST(BPlusTreeRemoveTest, HandlesSimpleRemoveNoUnderflow) {
   // max_size=4, min_size=2。节点有3个元素，删除1个后还有2个，不下溢。
   bptree::BPlusTree<int, int> tree(4, 4);
   tree.Insert(10, 100);
   tree.Insert(20, 200);
   tree.Insert(30, 300);

   // 删除存在的键
   tree.Remove(20);

   int value;
   EXPECT_TRUE(tree.Get_Value(10, &value));
   EXPECT_FALSE(tree.Get_Value(20, &value)); // 确认20已被删除
   EXPECT_TRUE(tree.Get_Value(30, &value));

   // 删除不存在的键，树结构应不变
   tree.Remove(50);
   EXPECT_TRUE(tree.Get_Value(10, &value));
   EXPECT_TRUE(tree.Get_Value(30, &value));
}

// 测试2：删除导致下溢，从右兄弟借用 (重分配)
TEST(BPlusTreeRemoveTest, HandlesRemoveWithRedistributionFromRight) {
   // max_size=4, min_size=2。
   bptree::BPlusTree<int, int> tree(4, 4);

   // 构造一个左节点有2个元素，右节点有3个元素的场景
   // 结构: 根[30] -> 左叶[10, 20], 右叶[30, 40, 50]
   tree.Insert(10, 100);
   tree.Insert(20, 200);
   tree.Insert(30, 300);
   tree.Insert(40, 400);
   tree.Insert(50, 500);

   // 从左叶删除20，导致其下溢 (size=1 < min_size=2)
   // 此时它应该从右兄弟[30,40,50]借一个元素(30)
   tree.Remove(20);

   int value;
   // 验证键的状态
   EXPECT_TRUE(tree.Get_Value(10, &value));
   EXPECT_FALSE(tree.Get_Value(20, &value)); // 20 已删除
   EXPECT_TRUE(tree.Get_Value(30, &value)); // 30 被借过来了
   EXPECT_TRUE(tree.Get_Value(40, &value));
   EXPECT_TRUE(tree.Get_Value(50, &value));

   // 验证迭代器顺序
   std::vector<int> expected_keys = {10, 30, 40, 50};
   int i = 0;
   for (const auto &pair: tree) {
       EXPECT_EQ(pair.first, expected_keys[i++]);
   }
}

// 测试3：删除导致下溢，从左兄弟借用 (重分配)
TEST(BPlusTreeRemoveTest, HandlesRemoveWithRedistributionFromLeft) {
   bptree::BPlusTree<int, int> tree(4, 4);

   // 构造一个左节点有3个元素，右节点有2个元素的场景
   // 结构: 根[30] -> 左叶[10, 20, 25], 右叶[30, 40]
   tree.Insert(20, 200);
   tree.Insert(25, 250);
   tree.Insert(30, 300);
   tree.Insert(40, 400);
   tree.Insert(10, 100);

   // 从右叶删除40，导致其下溢 (size=1 < min_size=2)
   // 此时它应该从左兄弟[10,20,25]借一个元素(25)
   tree.Remove(40);

   int value;
   EXPECT_TRUE(tree.Get_Value(10, &value));
   EXPECT_TRUE(tree.Get_Value(20, &value));
   EXPECT_TRUE(tree.Get_Value(25, &value)); // 25被借过来了
   EXPECT_TRUE(tree.Get_Value(30, &value));
   EXPECT_FALSE(tree.Get_Value(40, &value)); // 40 已删除

   std::vector<int> expected_keys = {10, 20, 25, 30};
   int i = 0;
   for (const auto &pair: tree) {
       EXPECT_EQ(pair.first, expected_keys[i++]);
   }
}


// 测试4：删除导致下溢，且兄弟节点无法借用，触发与左兄弟合并
TEST(BPlusTreeRemoveTest, HandlesRemoveWithMergeWithLeftSibling) {
   bptree::BPlusTree<int, int> tree(4, 4); // min_size=2

   // 构造一个左右兄弟都只有2个元素的场景
   // 结构: 根[30] -> 左叶[10, 20], 右叶[30, 40]
   tree.Insert(10, 100);
   tree.Insert(20, 200);
   tree.Insert(30, 300);
   tree.Insert(40, 400);

   // 从右叶删除40，导致其下溢 (size=1)，且左兄弟也无法借用
   // 此时右叶[30]应合并到左叶[10,20]中
   tree.Remove(40);

   int value;
   EXPECT_TRUE(tree.Get_Value(10, &value));
   EXPECT_TRUE(tree.Get_Value(20, &value));
   EXPECT_TRUE(tree.Get_Value(30, &value));
   EXPECT_FALSE(tree.Get_Value(40, &value));

   // 验证迭代器，此时所有元素应在一个叶子节点中
   std::vector<int> expected_keys = {10, 20, 30};
   int i = 0;
   for (const auto &pair: tree) {
       EXPECT_EQ(pair.first, expected_keys[i++]);
   }
   // 此时根节点应该为空，树的高度降低
}

// 测试5：删除导致级联合并，树的高度降低
TEST(BPlusTreeRemoveTest, HandlesRemoveWithCascadingMerge) {
   bptree::BPlusTree<int, int> tree(4, 4);

   // 插入7个元素，构造一个3层树
   std::vector<int> keys = {10, 20, 30, 40, 50, 60, 70};

   for (int key: keys) {
       tree.Insert(key, key * 10);
   }

   // 此时结构大致为：
   //       根[30, 50]
   //      /    |     \
   //  [20]    [40]    [60]
   //  /  \    /  \    /  \
   // 10. 20 .30 .40 .50. .60,70

   // 删除70, 60. 这将导致最右边的叶子节点合并，
   // 进而导致父节点[60]下溢并与兄弟[40]合并，
   // 再导致根节点[30,50]下溢，最终树高降低
   tree.Remove(70);
   tree.Remove(60);

   int value;
   EXPECT_FALSE(tree.Get_Value(60, &value));
   EXPECT_FALSE(tree.Get_Value(70, &value));
   EXPECT_TRUE(tree.Get_Value(50, &value));

   // 验证所有剩余元素
   std::vector<int> expected_keys = {10, 20, 30, 40, 50};
   int i = 0;
   for (const auto &pair: tree) {
       EXPECT_EQ(pair.first, expected_keys[i++]);
   }
   EXPECT_EQ(i, 5);
}

// 测试6：删除导致根节点清空
TEST(BPlusTreeRemoveTest, HandlesRemovingLastElement) {
   bptree::BPlusTree<int, int> tree(4, 4);
   tree.Insert(10, 100);
   tree.Remove(10);

   EXPECT_TRUE(tree.Is_Empty());

   int value;
   EXPECT_FALSE(tree.Get_Value(10, &value));
}

//范围查找
TEST(BPlusTreeIteratorTest, HandlesExplicitRangeScan) {
   bptree::BPlusTree<int, int> tree(3, 3);
   for (int i = 1; i <= 10; ++i) {
       tree.Insert(i, i * 10);
   }

   // 查找范围 [3, 7)
   auto result = tree.Range_Scan(3, 7);

   std::vector<std::pair<int, int>> expected = {
           {3, 30},
           {4, 40},
           {5, 50},
           {6, 60}
   };

   ASSERT_EQ(result.size(), expected.size());
   for (size_t i = 0; i < result.size(); ++i) {
       EXPECT_EQ(result[i].first, expected[i].first);
       EXPECT_EQ(result[i].second, expected[i].second);
   }
}