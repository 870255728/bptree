#pragma once

#include <functional> // for std::less
#include <iterator>   // for std::iterator_traits
#include <memory>     // for std::unique_ptr, std::make_unique
#include <stdexcept>  // for std::runtime_error
#include <utility>    // for std::pair, std::move
#include <vector>

#include "internal_node.h"
#include "leaf_node.h"
#include "node.h"
#include "types.h"

namespace bptree {

    template<typename KeyT, typename ValueT, typename KeyComparator = std::less<KeyT>>
    class BPlusTree {
    public:
        // --- 类型别名 ---
        using KeyType = KeyT;
        using ValueType = ValueT;
        using LeafNodeT = LeafNode<KeyT, ValueT, KeyComparator>;
        using InternalNodeT = InternalNode<KeyT, ValueT, KeyComparator>;
        using NodeT = Node<KeyT, ValueT, KeyComparator>;

        // =====================================================================
        // B+Tree Iterator Class Definition (作为嵌套类)
        // =====================================================================
        class BPlusTreeIterator {
        public:
            // C++迭代器协议所需的类型定义
            using value_type = std::pair<const KeyType, ValueType>;
            using pointer = const value_type *; // 指向const pair

            // 默认构造函数，创建一个“结束”迭代器
            BPlusTreeIterator() : current_leaf_(nullptr), index_in_leaf_(0) {}

            // 解引用操作符
            auto operator*() const -> value_type { // 返回一个右值
                return {current_leaf_->KeyAt(index_in_leaf_),
                        current_leaf_->Value_At(index_in_leaf_)};
            }

            /**
             * @brief 箭头操作符。
             * 这个操作符必须返回一个指针。因为我们不能安全地返回一个指向
             * 临时对象的指针，所以我们将它删除，并强制用户使用 (*it).first。
             * 这是一种安全的设计。
             */
            auto operator->() const -> pointer = delete; // 显式删除 operator->

            // 前缀自增
            auto operator++() -> BPlusTreeIterator & {
                if (current_leaf_ == nullptr) {
                    return *this;
                }
                index_in_leaf_++;
                if (index_in_leaf_ >= current_leaf_->Get_Size()) {
                    current_leaf_ = current_leaf_->Get_Next_Leaf();
                    index_in_leaf_ = 0;
                }
                return *this;
            }

            // 后缀自增
            auto operator++(int) -> BPlusTreeIterator {
                BPlusTreeIterator temp = *this;
                ++(*this);
                return temp;
            }

            // 比较操作
            auto operator==(const BPlusTreeIterator &other) const -> bool {
                return current_leaf_ == other.current_leaf_ && index_in_leaf_ == other.index_in_leaf_;
            }

            auto operator!=(const BPlusTreeIterator &other) const -> bool {
                return !(*this == other);
            }

        private:
            // 允许外围类 BPlusTree 调用此私有构造函数
            friend class BPlusTree;

            BPlusTreeIterator(LeafNodeT *leaf, int index)
                    : current_leaf_(leaf), index_in_leaf_(index) {}

            LeafNodeT *current_leaf_;
            int index_in_leaf_;
        };
        // ================== 迭代器定义结束 ==================

        // B+Tree 公共接口使用的迭代器类型
        using Iterator = BPlusTreeIterator;

        // --- B+Tree 构造函数 ---
        explicit BPlusTree(int leaf_max_size, int internal_max_size)
                : leaf_max_size_(leaf_max_size),
                  internal_max_size_(internal_max_size),
                  comparator_(),
                  root_(nullptr) {}

        // --- 公共方法 ---

        auto Is_Empty() const -> bool { return root_ == nullptr; }

        auto Insert(const KeyType &key, const ValueType &value) -> bool {
            if (this->Is_Empty()) {
                root_ = std::make_unique<LeafNodeT>(leaf_max_size_);
                auto *leaf_root = static_cast<LeafNodeT *>(root_.get());
                leaf_root->Insert(key, value, comparator_);
                return true;
            }

            NodeT *current = root_.get();
            std::vector<NodeT *> path;
            path.push_back(current);

            while (!current->Is_Leaf()) {
                auto *internal_node = static_cast<InternalNodeT *>(current);
                current = internal_node->Lookup(key, comparator_);
                path.push_back(current);
            }

            auto *leaf_to_insert = static_cast<LeafNodeT *>(current);

            int old_size = leaf_to_insert->Get_Size();
            leaf_to_insert->Insert(key, value, comparator_);

            if (leaf_to_insert->Get_Size() == old_size) {
                return false;
            }

            if (leaf_to_insert->IS_Full()) {
                this->Handle_Split(path);
            }

            return true;
        }

        auto Get_Value(const KeyType &key, ValueType *value) const -> bool {
            if (this->Is_Empty()) {
                return false;
            }
            auto *leaf = this->Find_Leaf(key);
            if (leaf == nullptr) {
                return false;
            }
            return leaf->Get_Value(key, value, comparator_);
        }

        auto Begin() -> Iterator {
            if (this->Is_Empty()) {
                return this->End();
            }
            auto *current = root_.get();
            while (!current->Is_Leaf()) {
                current = static_cast<InternalNodeT *>(current)->Child_At(0);
            }
            return Iterator(static_cast<LeafNodeT *>(current), 0);
        }

        auto End() -> Iterator {
            return Iterator();
        }

        /**
         * @brief 为范围 for 循环提供 begin() 接口
         * C++ 的范围 for 循环需要名为 begin() (小写) 的函数。
         * 我们让它直接调用我们已经实现好的 Begin() (大写)。
         */
        auto begin() -> Iterator {
            return this->Begin();
        }

        /**
         * @brief 为范围 for 循环提供 end() 接口
         */
        auto end() -> Iterator {
            return this->End();
        }

        /**
         * @brief 从树中删除一个键
         */
        void Remove(const KeyType &key) {
            if (this->Is_Empty()) {
                return;
            }

            // 1. 查找并记录路径 (保持不变)
            std::vector<NodeT *> path;
            NodeT *current = root_.get();
            while (true) {
                path.push_back(current);
                if (current->Is_Leaf()) break;
                current = static_cast<InternalNodeT *>(current)->Lookup(key, comparator_);
            }
            auto *leaf = static_cast<LeafNodeT *>(current);

            // 2. 在叶子中删除 (保持不变)
            int old_size = leaf->Get_Size();
            leaf->Remove(key, comparator_);
            if (leaf->Get_Size() == old_size) {
                return; // 键不存在
            }

            // 3. --- [ 修正这里的判断逻辑 ] ---
            // 只有当节点删除后仍然满足最小尺寸要求时，我们才提前退出。
            // 根节点是一个特例，如果它变空了，也需要处理，所以不能简单地用 leaf == root_.get() 来判断。
            if (!leaf->Is_Underflow()) {
                return;
            }

            // 4. 处理下溢 (现在变空的根节点也能进入这里了)
            this->Handle_Underflow(path);
        }

        /**
     * @brief 查找一个指定范围内的所有键值对
     * @param start_key 范围的起始键 (包含)
     * @param end_key 范围的结束键 (不包含)
     * @return 包含结果的vector
     */
        auto
        Range_Scan(const KeyType &start_key, const KeyType &end_key) -> std::vector<std::pair<KeyType, ValueType>> {
            std::vector<std::pair<KeyType, ValueType>> result;

            // 1. 找到起始位置的迭代器
            // (需要先实现一个 Begin(key) 的版本)
            Iterator it = this->Begin(start_key);

            // 2. 遍历直到结束或超出范围
            for (; it != this->End(); ++it) {
                if (comparator_((*it).first, end_key)) { // if current_key < end_key
                    result.push_back({(*it).first, (*it).second});
                } else {
                    // 因为是顺序遍历，一旦超出end_key，后续的也一定超出
                    break;
                }
            }
            return result;
        }

        /**
         * @brief 返回一个指向第一个不小于给定键的元素的迭代器
         */
        auto Begin(const KeyType &key) -> Iterator {
            if (Is_Empty()) {
                return End();
            }
            LeafNodeT *leaf = Find_Leaf(key);
            int index = leaf->Find_Key_Index(key, comparator_);
            return Iterator(leaf, index);
        }


    private:
        // --- 私有辅助函数 ---

        auto Find_Leaf(const KeyType &key) const -> LeafNodeT * {
            if (this->Is_Empty()) {
                return nullptr;
            }
            NodeT *current = root_.get();
            while (!current->Is_Leaf()) {
                auto *internal_node = static_cast<InternalNodeT *>(current);
                current = internal_node->Lookup(key, comparator_);
            }
            return static_cast<LeafNodeT *>(current);
        }

        void Handle_Split(std::vector<NodeT *> &path) {
            NodeT *node_to_check = path.back();
            path.pop_back();

            if (!node_to_check->IS_Full()) {
                return; // 如果节点没满，不需要分裂
            }

            // --- 执行分裂 ---
            KeyType key_to_parent;
            std::unique_ptr<NodeT> new_sibling_node;

            if (node_to_check->Is_Leaf()) {
                auto *leaf = static_cast<LeafNodeT *>(node_to_check);
                auto new_sibling = std::make_unique<LeafNodeT>(leaf_max_size_);
                key_to_parent = leaf->Split(new_sibling.get());

                new_sibling->Set_Next_Leaf(leaf->Get_Next_Leaf());
                leaf->Set_Next_Leaf(new_sibling.get());

                new_sibling_node = std::move(new_sibling);
            } else {
                auto *internal = static_cast<InternalNodeT *>(node_to_check);
                auto new_sibling = std::make_unique<InternalNodeT>(internal_max_size_);
                key_to_parent = internal->Split(new_sibling.get());
                new_sibling_node = std::move(new_sibling);
            }

            // --- 插入到父节点或创建新根 ---
            NodeT *parent = path.empty() ? nullptr : path.back();

            if (parent == nullptr) {
                // 根节点分裂了
                auto new_root = std::make_unique<InternalNodeT>(internal_max_size_);
                new_root->Populate_New_Root(key_to_parent, std::move(root_), std::move(new_sibling_node));
                root_ = std::move(new_root);
                return;
            }

            // 插入到父节点
            auto *internal_parent = static_cast<InternalNodeT *>(parent);
            internal_parent->Insert(key_to_parent, std::move(new_sibling_node), comparator_);

            // 递归检查父节点是否需要分裂
            if (internal_parent->IS_Full()) {
                this->Handle_Split(path);
            }
        }

        // --- 成员变量 ---
        int leaf_max_size_;
        int internal_max_size_;
        KeyComparator comparator_;
        std::unique_ptr<NodeT> root_;

        /**
         * @brief 处理节点下溢（重分配或合并）
         */
        void Handle_Underflow(std::vector<NodeT *> &path) {
            NodeT *node = path.back();
            path.pop_back();

            // 如果下溢的节点是根，特殊处理
            if (node == root_.get()) {
                if (!node->Is_Leaf() && node->Get_Size() == 0) {
                    auto *old_root = static_cast<InternalNodeT *>(root_.release());
                    root_ = old_root->Move_First_Child();
                    delete old_root;
                } else if (node->Is_Leaf() && node->Get_Size() == 0) {
                    root_.reset(nullptr);
                }
                return;
            }

            auto *parent = static_cast<InternalNodeT *>(path.back());
            int node_index_in_parent = parent->Find_Child_Index(node);

            // 尝试从左兄弟借
            if (node_index_in_parent > 0) {
                auto *left_sibling = parent->Child_At(node_index_in_parent - 1);
                if (left_sibling->Get_Size() > left_sibling->Get_Min_Size()) {
                    int parent_key_index = node_index_in_parent - 1;
                    if (node->Is_Leaf()) {
                        static_cast<LeafNodeT *>(node)->Move_Last_From(static_cast<LeafNodeT *>(left_sibling), parent,
                                                                       parent_key_index);
                    } else {
                        static_cast<InternalNodeT *>(node)->Move_Last_From(static_cast<InternalNodeT *>(left_sibling),
                                                                           parent, parent_key_index);
                    }
                    return;
                }
            }

            // 尝试从右兄弟借
            if (node_index_in_parent < parent->Get_Size()) {
                auto *right_sibling = parent->Child_At(node_index_in_parent + 1);
                if (right_sibling->Get_Size() > right_sibling->Get_Min_Size()) {
                    int parent_key_index = node_index_in_parent;
                    if (node->Is_Leaf()) {
                        static_cast<LeafNodeT *>(node)->Move_First_From(static_cast<LeafNodeT *>(right_sibling), parent,
                                                                        parent_key_index);
                    } else {
                        static_cast<InternalNodeT *>(node)->Move_First_From(static_cast<InternalNodeT *>(right_sibling),
                                                                            parent, parent_key_index);
                    }
                    return;
                }
            }

            // 无法借用，执行合并
            if (node_index_in_parent > 0) {
                // 与左兄弟合并 (将 node 合并到 left_sibling)
                auto *left_sibling = static_cast<NodeT *>(parent->Child_At(node_index_in_parent - 1));
                int parent_key_index = node_index_in_parent - 1;
                if (node->Is_Leaf()) {
                    static_cast<LeafNodeT *>(left_sibling)->Merge(static_cast<LeafNodeT *>(node));
                } else {
                    static_cast<InternalNodeT *>(left_sibling)->Merge_Into(static_cast<InternalNodeT *>(node),
                                                                           parent_key_index, parent);
                }
                parent->Remove_At(parent_key_index);
            } else {
                // 与右兄弟合并 (将 right_sibling 合并到 node)
                auto *right_sibling = static_cast<NodeT *>(parent->Child_At(node_index_in_parent + 1));
                int parent_key_index = node_index_in_parent;
                if (node->Is_Leaf()) {
                    static_cast<LeafNodeT *>(node)->Merge(static_cast<LeafNodeT *>(right_sibling));
                } else {
                    static_cast<InternalNodeT *>(node)->Merge_Into(static_cast<InternalNodeT *>(right_sibling),
                                                                   parent_key_index, parent);
                }
                parent->Remove_At(parent_key_index);
            }

            // 递归检查父节点
            if (parent->Is_Underflow()) {
                this->Handle_Underflow(path);
            }
        }
    };
} // namespace bptree