//
// Created by lenovo on 2025/8/5.
//

#ifndef BPTREE_LEAF_NODE_H
#define BPTREE_LEAF_NODE_H

#include <utility> // For std::pair
#include <vector>
#include <iterator>
#include <algorithm>
#include <memory>
#include "node.h"
#include "internal_node.h"
#include "bptree_node.pb.h"
#include "config.h"

namespace bptree {
    template<class KeyT, class ValueT, class KeyComparator>
    class LeafNode : public Node<KeyT, ValueT, KeyComparator> {
    public:

        using InternalNodeT = InternalNode<KeyT, ValueT, KeyComparator>;

        // 从基类继承类型别名，方便在本类和外部使用
        using Base = Node<KeyT, ValueT, KeyComparator>;
        using KeyType = typename Base::KeyType;
        using ValueType = typename Base::ValueType;

        // 引入基类的保护成员到当前作用域，方便使用
        using Base::keys_;
        using Base::Get_Size;
        using Base::Set_Size;
        using Base::Get_MAx_Size;

        /**
         * @brief 叶子节点构造函数
         * @param max_size 阶数
         */
        explicit LeafNode(int max_size) : Base(true, max_size) { // Node(bool is_leaf, int max_size)
            values_.reserve(max_size);
            // 初始化 next_page_id_ 为无效
            next_page_id_ = INVALID_PAGE_ID;
        }

        ~LeafNode() override = default;

        /**
         * @brief 在节点内查找指定键对应的值
         * @param key 要查找的键
         * @param value 如果找到，用于存储值的指针
         * @param comparator 键的比较器
         * @return 找到键:true
         */
        auto Get_Value(const KeyType &key, ValueType *value, const KeyComparator &comparator) const -> bool {
            int index = Find_Key_Index(key, comparator);
            if (index < Get_Size() && !comparator(key, this->keys_[index])) {
                *value = values_[index];
                return true;
            }
            return false;
        }

        /**
         * @brief 获取指定索引处的值
         * @param index 索引
         * @return 对应索引的值
         */
        auto Value_At(int index) const -> ValueType {
            return values_[index];
        }

        /**
         * @brief 在节点内插入一个新的键值对
         * @param key 要插入的键
         * @param value 要插入的值
         * @param comparator 键的比较器
         * @return 插入后节点的大小
         */
        auto Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) -> int {
            int index = Find_Key_Index(key, comparator);

            if (index < Get_Size() && !comparator(key, this->keys_[index])) {
                return Get_Size(); // 不插入重复键
            }

            this->keys_.insert(this->keys_.begin() + index, key);
            values_.insert(values_.begin() + index, value);

            this->Set_Size(this->Get_Size() + 1);
            return this->Get_Size();
        }

        /**
         * @brief 从节点中移除一个键值对
         * @param key
         * @param comparator
         * @return
         */
        auto Remove(const KeyType &key, const KeyComparator &comparator) -> int {
            int index = Find_Key_Index(key, comparator);

            if (index < Get_Size() && !comparator(key, this->keys_[index])) {
                this->keys_.erase(this->keys_.begin() + index);
                values_.erase(values_.begin() + index);
                this->Set_Size(this->Get_Size() - 1);
            }

            return this->Get_Size();
        }

        /**
         * @brief 将当前节点后半部分的键值对移动到给定的新节点 (recipient)
         * @param recipient 接收后半部分数据的新创建的叶子节点
         */
        // in include/bptree/leaf_node.h
        auto Split(LeafNode *recipient) -> KeyType {
            int split_point = Get_MAx_Size() / 2;
            KeyType new_key_to_parent = this->keys_[split_point];

            // 1. 移动键和值
            recipient->keys_.assign(std::make_move_iterator(this->keys_.begin() + split_point),
                                    std::make_move_iterator(this->keys_.end()));
            recipient->values_.assign(std::make_move_iterator(this->values_.begin() + split_point),
                                      std::make_move_iterator(this->values_.end()));

            // 2. 删除当前节点已移动的数据
            this->keys_.erase(this->keys_.begin() + split_point, this->keys_.end());
            this->values_.erase(this->values_.begin() + split_point, this->values_.end());

            // 3. [修复] 根据 vector 的实际大小来更新 size_ 成员
            this->Set_Size(this->keys_.size());
            recipient->Set_Size(recipient->keys_.size());

            return new_key_to_parent;
        }

        /**
         * @brief 获取下一个叶子节点的 Page ID
         * @return 下一个叶子节点的 Page ID
         */
        auto Get_Next_Page_Id() const -> page_id_t {
            return next_page_id_;
        }

        /**
         * @brief 设置下一个叶子节点的 Page ID
         * @param next_page_id 指向下一个叶子节点的 Page ID
         */
        void Set_Next_Page_Id(page_id_t next_page_id) {
            next_page_id_ = next_page_id;
        }


        /**
         * @brief 将左兄弟的最后一个元素移动到本节点开头
         */
        void Move_Last_From(LeafNode *sibling, InternalNodeT *parent, int parent_key_index) {
            // 1. 移动键值对
            this->keys_.insert(this->keys_.begin(), sibling->keys_.back());
            this->values_.insert(this->values_.begin(), sibling->values_.back());
            sibling->keys_.pop_back();
            sibling->values_.pop_back();

            // 2. 更新大小
            this->Set_Size(this->Get_Size() + 1);
            sibling->Set_Size(sibling->Get_Size() - 1);

            // 3. 更新父节点的分隔键为本节点的新首键
            parent->Set_Key_At(parent_key_index, this->keys_.front());
        }

        /**
         * @brief 将右兄弟的第一个元素移动到本节点末尾
         * @param sibling 右兄弟
         * @param parent 父节点
         * @param parent_key_index 当前节点在父节点的左边还是右边
         */
        void Move_First_From(LeafNode *sibling, InternalNodeT *parent, int parent_key_index) {
            // 1. 移动键值对
            this->keys_.push_back(sibling->keys_.front());
            this->values_.push_back(sibling->values_.front());
            sibling->keys_.erase(sibling->keys_.begin());
            sibling->values_.erase(sibling->values_.begin());

            // 2. 更新大小
            this->Set_Size(this->Get_Size() + 1);
            sibling->Set_Size(sibling->Get_Size() - 1);

            // 3. 更新父节点的分隔键为右兄弟的新首键
            parent->Set_Key_At(parent_key_index, sibling->keys_.front());
        }

        /**
        * @brief 将另一个叶子节点的所有数据合并到本节点
        * @param sibling 要合并进来的兄弟节点
        */
        void Merge(LeafNode *sibling) {
            // 将兄弟节点的数据移动到本节点末尾
            this->keys_.insert(this->keys_.end(),
                               std::make_move_iterator(sibling->keys_.begin()),
                               std::make_move_iterator(sibling->keys_.end()));
            this->values_.insert(this->values_.end(),
                                 std::make_move_iterator(sibling->values_.begin()),
                                 std::make_move_iterator(sibling->values_.end()));

            // 更新大小和链表指针
            this->Set_Size(this->Get_Size() + sibling->Get_Size());
            this->Set_Next_Page_Id(sibling->Get_Next_Page_Id());
        }

        auto Find_Key_Index(const KeyType &key, const KeyComparator &comparator) const -> int {
            auto it = std::lower_bound(this->keys_.begin(), this->keys_.end(), key, comparator); // 有序范围内查找第一个不小于给定值的位置
            return std::distance(this->keys_.begin(), it);
        }

        void Serialize(char* page_data) const {
            LeafNodeProto proto;
            // 1. 填充头部
            proto.mutable_header()->set_is_leaf(this->Is_Leaf());
            proto.mutable_header()->set_size(this->Get_Size());
            proto.mutable_header()->set_max_size(this->Get_MAx_Size());
            proto.set_next_page_id(this->next_page_id_);

            // 2. 填充数据 (使用 a dd_... 或 a ssign)
            proto.mutable_keys()->Add(this->keys_.begin(), this->keys_.end());
            proto.mutable_values()->Add(this->values_.begin(), this->values_.end());

            // 3. 序列化到页面数据区
            if (!proto.SerializeToArray(page_data, PAGE_SIZE)) {
                // 处理序列化失败（例如，数据太大）
                throw std::runtime_error("Failed to serialize LeafNode.");
            }
        }

        void Deserialize(const char* page_data, int size) {
            LeafNodeProto proto;
            if (!proto.ParseFromArray(page_data, size)) {
                throw std::runtime_error("Failed to deserialize LeafNode.");
            }

            // 1. 从头部恢复元数据
            this->Set_Size(proto.header().size());
            this->next_page_id_ = proto.next_page_id();

            // 2. 恢复数据
            this->keys_.assign(proto.keys().begin(), proto.keys().end());
            this->values_.assign(proto.values().begin(), proto.values().end());
        }

    private:
        std::vector<ValueType> values_;
        page_id_t next_page_id_;
    };
}


#endif //BPTREE_LEAF_NODE_H
