//
// Created by lenovo on 2025/8/5.
//

#ifndef BPTREE_NODE_H
#define BPTREE_NODE_H

#include <vector>
#include <memory>
#include "config.h"

/**
 * @brief 基类节点
 */
namespace bptree {

    /**
     * @brief B+Tree 节点页面的通用头部格式
     *
     * 这个结构体定义了所有节点页面开头共有的元数据。
     * 它的总大小必须小于 PAGE_SIZE。
     */
    struct NodeHeader {
        // 页面存储的节点类型 (叶子或内部)
        bool is_leaf_;
        // 当前存储的键的数量
        int size_;
    };


    /**
     * @brief B+Tree 节点基类 (页面视图)
     *
     * 这个类不再拥有自己的数据成员，而是提供了一组接口
     * 来解释和操作一块原始的内存区域 (一个 Page 的数据区)。
     *
     * @tparam KeyT 键的类型
     * @tparam ValueT 值的类型
     * @tparam KeyComparator 键的比较器类型
     */
    template<typename KeyT, typename ValueT, typename KeyComparator>
    class Node {
    public:
        using KeyType = KeyT;
        using ValueType = ValueT;

        // --- 构造函数/析构函数 ---
        // 默认的构造和析构即可，因为它不管理任何资源
        Node() = default;
        virtual ~Node() = default;

        // --- 页面初始化 ---
        /**
         * @brief 在给定的页面内存上初始化节点头部
         * @param page_data 指向 Page 数据区的指针
         * @param is_leaf 节点是否是叶子
         * @param max_size 节点的最大键数
         */
        void Init(char* page_data, bool is_leaf) {
            // 在页面开头设置头部信息
            Set_Is_Leaf(page_data, is_leaf);
            Set_Size(page_data, 0);
            // max_size 是逻辑上的，通常需要存储在页面中
            // 我们在叶子和内部节点中分别处理
        }

        // --- 元数据访问 (Setter/Getter) ---

        auto Is_Leaf(const char* page_data) const -> bool {
            return reinterpret_cast<const NodeHeader*>(page_data)->is_leaf_;
        }

        void Set_Is_Leaf(char* page_data, bool is_leaf) {
            reinterpret_cast<NodeHeader*>(page_data)->is_leaf_ = is_leaf;
        }

        auto Get_Size(const char* page_data) const -> int {
            return reinterpret_cast<const NodeHeader*>(page_data)->size_;
        }

        void Set_Size(char* page_data, int size) {
            reinterpret_cast<NodeHeader*>(page_data)->size_ = size;
        }

        // --- 逻辑判断 ---

        /**
         * @brief 获取节点的最小键数
         */
        auto Get_Min_Size(int max_size) const -> int {
            return (max_size + 1) / 2;
        }

        /**
         * @brief 判断节点的键数是否过少 (下溢)
         */
        auto Is_Underflow(const char* page_data, int max_size) const -> bool {
            return Get_Size(page_data) < Get_Min_Size(max_size);
        }

        /**
         * @brief 当前节点是否已满
         */
        auto IS_Full(const char* page_data, int max_size) const -> bool {
            return Get_Size(page_data) >= max_size;
        }

        // KeyAt 需要在子类中实现，因为键的存储位置依赖于节点类型
        // virtual auto KeyAt(const char* page_data, int index) const -> KeyType = 0;
    };

}

#endif //BPTREE_NODE_H
