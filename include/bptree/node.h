#ifndef BPTREE_NODE_H
#define BPTREE_NODE_H

#include <vector>
#include <memory>
#include "config.h"

/**
 * @brief 基类节点
 */
namespace bptree {

    struct NodeHeader {
        // 页面存储的节点类型 (叶子或内部)
        bool is_leaf_;
        // 当前节点存储的键的数量
        int size_;
    };

    template<typename KeyT, typename ValueT, typename KeyComparator>
    class Node {
    public:
        using KeyType = KeyT;
        using ValueType = ValueT;

        Node() = default;

        virtual ~Node() = default;

        void Init(char *page_data, bool is_leaf) {
            Set_Is_Leaf(page_data, is_leaf);
            Set_Size(page_data, 0);
        }

        auto Is_Leaf(const char *page_data) const -> bool {
            return reinterpret_cast<const NodeHeader *>(page_data)->is_leaf_;
        }

        void Set_Is_Leaf(char *page_data, bool is_leaf) {
            reinterpret_cast<NodeHeader *>(page_data)->is_leaf_ = is_leaf;
        }

        auto Get_Size(const char *page_data) const -> int {
            return reinterpret_cast<const NodeHeader *>(page_data)->size_;
        }

        void Set_Size(char *page_data, int size) {
            reinterpret_cast<NodeHeader *>(page_data)->size_ = size;
        }

        auto Get_Min_Size(int max_size) const -> int {
            return (max_size + 1) / 2;
        }

        auto Is_Underflow(const char *page_data, int max_size) const -> bool {
            return Get_Size(page_data) < Get_Min_Size(max_size);
        }

        auto IS_Full(const char *page_data, int max_size) const -> bool {
            return Get_Size(page_data) >= max_size;
        }
    };

}

#endif
