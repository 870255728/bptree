//
// Created by lenovo on 2025/8/18.
//
#include "gtest/gtest.h"
#include "bptree_node.pb.h" // 包含我们生成的头文件
#include <string>

// 测试基本的序列化和反序列化功能
TEST(ProtobufTest, SerializationDeserialization) {
    // 1. 创建并填充一个 BPTreeNode 对象 (模拟一个叶子节点)
    bptree::BPTreeNode original_node;
    original_node.set_node_type(bptree::NODE_TYPE_LEAF);
    original_node.set_page_id(101);
    original_node.set_parent_page_id(5);
    original_node.set_next_sibling_id(102);
    original_node.set_key_count(2);

    // 添加键和值
    original_node.add_values(100);
    original_node.add_values(200);

    // 2. 序列化: 将对象转换为一个字符串 (字节流)
    std::string serialized_data;
    bool success = original_node.SerializeToString(&serialized_data);
    ASSERT_TRUE(success);
    ASSERT_FALSE(serialized_data.empty());

    // 打印序列化后的数据大小 (可选)
    std::cout << "Serialized node data size: " << serialized_data.size() << " bytes" << std::endl;

    // 3. 反序列化: 从字符串中恢复对象
    bptree::BPTreeNode deserialized_node;
    success = deserialized_node.ParseFromString(serialized_data);
    ASSERT_TRUE(success);

    // 4. 验证: 检查恢复后的对象是否与原始对象一致
    EXPECT_EQ(deserialized_node.node_type(), bptree::NODE_TYPE_LEAF);
    EXPECT_EQ(deserialized_node.page_id(), 101);
    EXPECT_EQ(deserialized_node.parent_page_id(), 5);
    EXPECT_EQ(deserialized_node.next_sibling_id(), 102);
    EXPECT_EQ(deserialized_node.key_count(), 2);

    ASSERT_EQ(deserialized_node.values_size(), 2);
    EXPECT_EQ(deserialized_node.values(0), 100);
    EXPECT_EQ(deserialized_node.values(1), 200);
}