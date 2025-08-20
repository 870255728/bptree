//
// Created by lenovo on 2025/8/5.
//

#ifndef BPTREE_INTERNAL_NODE_H
#define BPTREE_INTERNAL_NODE_H

#include <algorithm>
#include <iterator>
#include <vector>
#include <memory>
#include <node.h>
#include <stdexcept>
#include "bptree_node.pb.h"
#include "config.h"

namespace bptree {
    template<typename KeyT, typename ValueT, typename KeyComparator>
    class InternalNode : public Node<KeyT, ValueT, KeyComparator> {
    public:
        // 从基类继承类型别名，方便在本类和外部使用
        using Base = Node<KeyT, ValueT, KeyComparator>;
        using KeyType = typename Base::KeyType;
        using ChildPtr = page_id_t;

        // 引入基类的保护成员到当前作用域，方便使用
        using Base::keys_;
        using Base::Get_Size;
        using Base::Set_Size;
        using Base::Get_MAx_Size;

        using NodeT = Node<KeyT, ValueT, KeyComparator>;

        explicit InternalNode(int max_size) : Base(false, max_size) {
            children_.reserve(max_size + 1);
        }

        ~InternalNode() override = default;

        /**
         * @brief 根据给定的键，查找对应的子节点 Page ID
         * @param key 要查找的键
         * @param comparator 键的比较器
         */
        auto Lookup(const KeyType &key, const KeyComparator &comparator) const -> page_id_t {
            auto it = std::upper_bound(this->keys_.begin(), this->keys_.end(), key, comparator);
            int index = std::distance(this->keys_.begin(), it);
            return children_[index];
        }

        /**
         * @brief 获取指定索引处的子节点 Page ID
         * @param index 子节点的索引
         */
        auto Child_At(int index) const -> page_id_t {
            return children_[index];
        }

        // --- [ 3. 添加序列化/反序列化方法 ] ---

        /**
         * @brief 将当前节点的状态序列化到页面数据区
         * @param page_data 指向页面数据区的指针
         */
        void Serialize(char *page_data) const {
            InternalNodeProto proto;

            // 填充头部信息
            proto.mutable_header()->set_is_leaf(false);
            proto.mutable_header()->set_size(this->Get_Size());
            proto.mutable_header()->set_max_size(this->Get_MAx_Size());

            // 填充键和子节点ID
            // Google Protobuf 的 Add() 方法可以直接接收迭代器范围
            proto.mutable_keys()->Add(this->keys_.begin(), this->keys_.end());
            proto.mutable_children()->Add(this->children_.begin(), this->children_.end());

            // 序列化到提供的缓冲区
            if (!proto.SerializeToArray(page_data, PAGE_SIZE)) {
                throw std::runtime_error("Failed to serialize InternalNode: Data may be too large for page.");
            }
        }

        /**
         * @brief 从页面数据区反序列化来恢复节点状态
         * @param page_data 指向页面数据区的指针
         * @param size 数据的有效大小 (通常是PAGE_SIZE)
         */
        void Deserialize(const char *page_data, int size) {
            InternalNodeProto proto;
            if (!proto.ParseFromArray(page_data, size)) {
                throw std::runtime_error("Failed to deserialize InternalNode.");
            }

            // 恢复头部信息
            // is_leaf 和 max_size 在构造时已确定，主要是恢复 size
            this->Set_Size(proto.header().size());

            // 恢复键和子节点ID
            this->keys_.assign(proto.keys().begin(), proto.keys().end());
            this->children_.assign(proto.children().begin(), proto.children().end());
        }

        /**
         * @brief 插入一个新的键和它右侧的子节点
         * @param key 要插入的键
         * @param child_ptr 指向新子节点的 unique_ptr
         * @param comparator 键的比较器
         */
        void Insert(const KeyType &key, page_id_t child_page_id, const KeyComparator &comparator) {
            auto it = std::upper_bound(this->keys_.begin(), this->keys_.end(), key, comparator);
            int index = std::distance(this->keys_.begin(), it);
            this->keys_.insert(this->keys_.begin() + index, key);
            this->children_.insert(this->children_.begin() + index + 1, child_page_id);
            this->Set_Size(this->Get_Size() + 1);
        }

        /**
         * @brief 将当前节点分裂成两个节点
         * @param recipient 用于接收后半部分数据的新创建的内部节点
         * @return 分裂后推到父节点的键
         */
        auto Split(InternalNode *recipient) -> KeyType {
            int split_point = Get_MAx_Size() / 2;
            KeyType key_to_parent = this->keys_[split_point];

            // 1. 移动键和子节点 (保持不变)
            recipient->keys_.assign(std::make_move_iterator(this->keys_.begin() + split_point + 1),
                                    std::make_move_iterator(this->keys_.end()));
            recipient->children_.assign(std::make_move_iterator(this->children_.begin() + split_point + 1),
                                        std::make_move_iterator(this->children_.end()));

            // 2. 删除当前节点已移动的数据 (保持不变)
            this->keys_.erase(this->keys_.begin() + split_point, this->keys_.end());
            this->children_.erase(this->children_.begin() + split_point + 1, this->children_.end());

            // 3. [修复] 根据 vector 的实际大小来更新 size_ 成员
            // 必须在移动和删除之后进行
            this->Set_Size(this->keys_.size());
            recipient->Set_Size(recipient->keys_.size());

            return key_to_parent;
        }

        void Populate_New_Root(const KeyType &key, page_id_t old_root_id, page_id_t new_sibling_id) {
            // 这个方法在类的内部，所以可以访问私有成员
            this->keys_.push_back(key);
            this->children_.push_back(old_root_id);
            this->children_.push_back(new_sibling_id);
            this->Set_Size(1);
        }

        /**
         * @brief 查找一个子节点指针在 children_ 数组中的索引
         */
        auto Find_Child_Index(page_id_t child_id) const -> int {
            for (size_t i = 0; i < children_.size(); ++i) {
                if (children_[i] == child_id) {
                    return i;
                }
            }
            return -1; // Not found
        }

        /**
         * @brief 移除指定索引处的键和子节点指针
         */
        void Remove_At(int index) {
            this->keys_.erase(this->keys_.begin() + index);
            // 移除键右侧的子节点
            this->children_.erase(this->children_.begin() + index + 1);
            this->Set_Size(this->Get_Size() - 1);
        }

        /**
         * @brief 设置指定索引处的键
         */
        void Set_Key_At(int index, const KeyType &key) {
            this->keys_[index] = key;
        }

        /**
         * @brief 将左兄弟的最后一个元素(键+子指针)移动到本节点开头
         * @param sibling 左兄弟节点
         * @param parent 父节点
         * @param parent_key_index 父节点中分隔本节点和左兄弟的键的索引
         */
        void Move_Last_From(InternalNode *sibling, InternalNode *parent, int parent_key_index) {
            // 1. 从父节点拉下分隔键，插入到本节点开头
            this->keys_.insert(this->keys_.begin(), parent->KeyAt(parent_key_index));

            // 2. 将左兄弟的最后一个子节点移动到本节点开头
            this->children_.insert(this->children_.begin(), sibling->children_.back());
            sibling->children_.pop_back();

            // 3. 将左兄弟的最后一个键移动到父节点，替换原来的分隔键
            parent->Set_Key_At(parent_key_index, sibling->keys_.back());
            sibling->keys_.pop_back();

            // 4. 更新大小
            this->Set_Size(this->Get_Size() + 1);
            sibling->Set_Size(sibling->Get_Size() - 1);
        }

        /**
         * @brief 将右兄弟的第一个元素(键+子指针)移动到本节点末尾
         * @param sibling 右兄弟节点
         * @param parent 父节点
         * @param parent_key_index 父节点中分隔本节点和右兄弟的键的索引
         */
        void Move_First_From(InternalNode *sibling, InternalNode *parent, int parent_key_index) {
            // 1. 从父节点拉下分隔键，追加到本节点末尾
            this->keys_.push_back(parent->KeyAt(parent_key_index));

            // 2. 将右兄弟的第一个子节点移动到本节点末尾
            this->children_.push_back(sibling->children_.front());
            sibling->children_.erase(sibling->children_.begin());

            // 3. 将右兄弟的第一个键移动到父节点，替换原来的分隔键
            parent->Set_Key_At(parent_key_index, sibling->keys_.front());
            sibling->keys_.erase(sibling->keys_.begin());

            // 4. 更新大小
            this->Set_Size(this->Get_Size() + 1);
            sibling->Set_Size(sibling->Get_Size() - 1);
        }

        /**
         * @brief 将右兄弟的所有数据合并到本节点
         * @param sibling 右兄弟节点
         * @param parent_key_index 父节点中分隔键的索引
         * @param parent 父节点
         */
        void Merge_Into(InternalNode *sibling, int parent_key_index, InternalNode *parent) {
            // 1. 从父节点拉下分隔键，追加到本节点
            this->keys_.push_back(parent->KeyAt(parent_key_index));

            // 2. 将右兄弟的所有键和子节点移动到本节点
            this->keys_.insert(this->keys_.end(),
                               std::make_move_iterator(sibling->keys_.begin()),
                               std::make_move_iterator(sibling->keys_.end()));
            this->children_.insert(this->children_.end(),
                                   std::make_move_iterator(sibling->children_.begin()),
                                   std::make_move_iterator(sibling->children_.end()));

            // 3. 更新本节点大小
            this->Set_Size(this->Get_Size() + sibling->Get_Size() + 1);

            // 4. 从父节点中移除分隔键和指向右兄弟的指针
            parent->Remove_At(parent_key_index);
        }

        /**
         * @brief 移除并返回第一个子节点的所有权。
         * 这个函数主要用于当根节点下溢，树的高度需要降低时。
         */
        auto Move_First_Child() -> page_id_t {
            // 确保只有一个孩子
            // assert(this->Get_Size() == 0 && !children_.empty());
            return children_.front();
        }

    private:
        std::vector<page_id_t> children_;
    };
}

#endif //BPTREE_INTERNAL_NODE_H
