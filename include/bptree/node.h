//
// Created by lenovo on 2025/8/5.
//

#ifndef BPTREE_NODE_H
#define BPTREE_NODE_H

#include <vector>
#include <memory>
#include "types.h"

/**
 * @brief 基类节点
 */
namespace bptree {

    template<class KeyT, class ValueT, class KeyComparator>
    class Node {
    public:
        using KeyType = KeyT;
        using ValueType = ValueT;

        /**
         * @brief 构造函数
         * @param is_leaf 节点是否是叶子节点
         * @param max_size 节点的最大容量（度数）
         */
        explicit Node(bool is_leaf, int max_size)
                : is_leaf_(is_leaf), size_(0)
                , max_size_(max_size) {
            // 预分配内存以提高性能，避免多次重新分配
            keys_.reserve(max_size);
        }

        virtual ~Node() = default;

        /**
         * @brief 获取节点的最小键数
         * 节点至少半满。
         */
        auto Get_Min_Size() const -> int {
            // 如果max_size是最大键数，那么最小键数是 ceil(max_size / 2)
            int a = (max_size_ + 1) / 2;
            return a;
        }

        /**
         * @brief 判断节点的键数是否过少 (下溢)
         */
        auto Is_Underflow() const -> bool {
            // 根节点可以是空的或只有一个孩子，不下溢
            // （这个判断逻辑主要在BPlusTree类中处理）
            return size_ < Get_Min_Size();
        }

        /**
         * @brief 判断是否为叶子节点
         * @return 叶子:true
         */
        auto Is_Leaf() const -> bool{
            return is_leaf_;
        };

        /**
         * @brief 当前节点键的数量
         * @return int个数
         */
        auto Get_Size() const -> int{
            return size_;
        };

        /**
         * @brief 设置当前节点键数量
         * @param size 键数量
         */
        void Set_Size(int size){
            size_=size;
        };

        /**
         * @brief 当前节点最大键数(阶数)
         * @return 最大键数(阶数)
         */
        auto Get_MAx_Size() const -> int{
            return max_size_;
        };

        /**
         * @brief 当前节点是否已满
         * @return 满了:true
         */
        auto IS_Full() const -> bool{
            return size_>=max_size_;
        };

        /**
         * @brief 访问当前节点中第index个键
         * @param index
         * @return 对应索引的键
         */
        auto KeyAt(int index) const -> KeyType{
            return keys_[index];
        };

    protected:
        // 成员变量
        bool is_leaf_;
        int size_;
        int max_size_; //阶数
        std::vector<KeyType> keys_;
    };

}

#endif //BPTREE_NODE_H
