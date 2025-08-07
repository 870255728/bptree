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

namespace bptree {
    template<typename KeyT, typename ValueT, typename KeyComparator>
    class InternalNode : public Node<KeyT, ValueT, KeyComparator> {
    public:
        // 从基类继承类型别名，方便在本类和外部使用
        using Base = Node<KeyT, ValueT, KeyComparator>;
        using KeyType = typename Base::KeyType;
        using ValueType = typename Base::ValueType;
        using ChildPtr = std::unique_ptr<Base>;

        // 引入基类的保护成员到当前作用域，方便使用
        using Base::keys_;
        using Base::Get_Size;
        using Base::Set_Size;
        using Base::Get_MAx_Size;

        using NodeT = Node<KeyT, ValueT, KeyComparator>;

        explicit InternalNode(int max_size) : Base(false, max_size) {
            children_.reserve(max_size + 1);
        }


        /**
         * @brief 根据给定的键，查找对应的子节点指针
         * @param key 要查找的键
         * @param comparator 键的比较器
         * @return 指向应该继续搜索的子节点的原始指针
         */
        auto Lookup(const KeyType &key, const KeyComparator &comparator) const -> Base * {
            // std::upper_bound 找到第一个大于 key 的元素。
            // 其索引正好对应于应该进入的子节点的索引。
            auto it = std::upper_bound(this->keys_.begin(), this->keys_.end(), key, comparator);
            int index = std::distance(this->keys_.begin(), it);
            return children_[index].get();
        }

        /**
         * @brief 获取指定索引处的子节点指针
         * @param index 子节点的索引
         * @return 指向子节点的原始指针
         */
        auto Child_At(int index) const -> Base * {
            return children_[index].get();
        }

        // --- 修改操作 ---

        /**
         * @brief 插入一个新的键和它右侧的子节点
         * @param key 要插入的键
         * @param child_ptr 指向新子节点的 unique_ptr
         * @param comparator 键的比较器
         */
        void Insert(const KeyType &key, ChildPtr child_ptr, const KeyComparator &comparator) {
            auto it = std::upper_bound(this->keys_.begin(), this->keys_.end(), key, comparator);
            int index = std::distance(this->keys_.begin(), it);

            this->keys_.insert(this->keys_.begin() + index, key);
            this->children_.insert(this->children_.begin() + index + 1, std::move(child_ptr));
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

        void Populate_New_Root(const KeyType& key, ChildPtr old_root, ChildPtr new_sibling) {
            // 这个方法在类的内部，所以可以访问私有成员
            this->keys_.push_back(key);
            this->children_.push_back(std::move(old_root));
            this->children_.push_back(std::move(new_sibling));
            this->Set_Size(1);
        }

        /**
         * @brief 查找一个子节点指针在 children_ 数组中的索引
         */
        auto Find_Child_Index(NodeT* child) const -> int {
            for (size_t i = 0; i < children_.size(); ++i) {
                if (children_[i].get() == child) {
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
        void Set_Key_At(int index, const KeyType& key) {
            this->keys_[index] = key;
        }

        /**
         * @brief 将左兄弟的最后一个元素(键+子指针)移动到本节点开头
         * @param sibling 左兄弟节点
         * @param parent 父节点
         * @param parent_key_index 父节点中分隔本节点和左兄弟的键的索引
         */
        void Move_Last_From(InternalNode* sibling, InternalNode* parent, int parent_key_index) {
            // 1. 从父节点拉下分隔键，插入到本节点开头
            this->keys_.insert(this->keys_.begin(), parent->KeyAt(parent_key_index));

            // 2. 将左兄弟的最后一个子节点移动到本节点开头
            this->children_.insert(this->children_.begin(), std::move(sibling->children_.back()));
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
        void Move_First_From(InternalNode* sibling, InternalNode* parent, int parent_key_index) {
            // 1. 从父节点拉下分隔键，追加到本节点末尾
            this->keys_.push_back(parent->KeyAt(parent_key_index));

            // 2. 将右兄弟的第一个子节点移动到本节点末尾
            this->children_.push_back(std::move(sibling->children_.front()));
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
        void Merge_Into(InternalNode* sibling, int parent_key_index, InternalNode* parent) {
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
        auto Move_First_Child() -> ChildPtr {
            // 确保只有一个孩子
            // assert(this->Get_Size() == 0 && !children_.empty());
            return std::move(children_.front());
        }

    private:
        std::vector<ChildPtr> children_;
    };
}

#endif //BPTREE_INTERNAL_NODE_H
