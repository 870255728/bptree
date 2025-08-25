#pragma once

#include <functional> // for std::less
#include <iterator>   // for std::iterator_traits
#include <memory>     // for std::unique_ptr, std::make_unique
#include <stdexcept>  // for std::runtime_error
#include <utility>    // for std::pair, std::move
#include <vector>
#include <string>
#include <chrono>

#include "internal_node.h"
#include "leaf_node.h"
#include "node.h"
#include "config.h"
#include "buffer_pool_manager.h"
#include "page_guard.h"
#include "disk_manager.h"
#include "lru_replacer.h"

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
            BPlusTreeIterator() = default;

            auto operator*() const -> value_type {
                // Guard在解引用时临时创建，确保页面在内存中
                PageGuard guard = bpm_->FetchPageGuard(page_id_);
                LeafNodeT leaf_view;
                return {leaf_view.Keys_Ptr(guard.GetData())[index_in_leaf_],
                        leaf_view.Values_Ptr(guard.GetData(), leaf_max_size_)[index_in_leaf_]};
            }

            /**
             * @brief 箭头操作符。
             * 这个操作符必须返回一个指针。因为我们不能安全地返回一个指向
             * 临时对象的指针，所以我们将它删除，并强制用户使用 (*it).first。
             * 这是一种安全的设计。
             */
            auto operator->() const -> pointer = delete;

            // 前缀自增
            auto operator++() -> BPlusTreeIterator & {
                if (bpm_ == nullptr || page_id_ == INVALID_PAGE_ID) return *this;
                PageGuard guard = bpm_->FetchPageGuard(page_id_);
                LeafNodeT leaf_view;
                index_in_leaf_++;
                if (index_in_leaf_ >= leaf_view.Get_Size(guard.GetData())) {
                    page_id_ = leaf_view.Get_Next_Page_Id(guard.GetData());
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
                return bpm_ == other.bpm_ && page_id_ == other.page_id_ && index_in_leaf_ == other.index_in_leaf_;
            }

            auto operator!=(const BPlusTreeIterator &other) const -> bool { return !(*this == other); }

        private:
            friend class BPlusTree;

            BPlusTreeIterator(BufferPoolManager *bpm, page_id_t page_id, int index, int leaf_max_size)
                    : bpm_(bpm), page_id_(page_id), index_in_leaf_(index), leaf_max_size_(leaf_max_size) {}

            BufferPoolManager *bpm_{nullptr};
            page_id_t page_id_{INVALID_PAGE_ID};
            int index_in_leaf_{0};
            int leaf_max_size_{0};
        };
        // ================== 迭代器定义结束 ==================

        // B+Tree 公共接口使用的迭代器类型
        using Iterator = BPlusTreeIterator;

        // --- B+Tree 构造函数 ---
        explicit BPlusTree(const std::string &db_file, int leaf_max_size, int internal_max_size)
                : leaf_max_size_(leaf_max_size), internal_max_size_(internal_max_size), comparator_(),
                  db_file_name_(db_file), delete_db_on_destruct_(false) {

            disk_manager_ = std::make_unique<DiskManager>(db_file);
            replacer_ = std::make_unique<LRUReplacer>(POOL_SIZE);
            bpm_ = std::make_unique<BufferPoolManager>(POOL_SIZE, disk_manager_.get(), replacer_.get());

            // 使用 page_id 0 作为元数据页
            Page *meta_page = bpm_->FetchPage(0);
            root_page_id_ = *reinterpret_cast<page_id_t *>(meta_page->GetData());
            bpm_->UnpinPage(0, false);

            if (root_page_id_ == 0) { // Uninitialized
                root_page_id_ = INVALID_PAGE_ID;
            }
        }

        ~BPlusTree() {
            if (bpm_ != nullptr) {
                Page *meta_page = bpm_->FetchPage(0);
                *reinterpret_cast<page_id_t *>(meta_page->GetData()) = root_page_id_;
                bpm_->UnpinPage(0, true);
            }
            if (delete_db_on_destruct_) {
                // 释放持有的文件句柄后再删除文件
                bpm_.reset();
                replacer_.reset();
                disk_manager_.reset();
                if (!db_file_name_.empty()) {
                    std::remove(db_file_name_.c_str());
                }
            }
        }

        // 纯内存模式构造（用于阶段一单测/基准）
        explicit BPlusTree(int leaf_max_size, int internal_max_size)
                : leaf_max_size_(leaf_max_size), internal_max_size_(internal_max_size), comparator_() {
            // 生成唯一的临时文件名，生命周期内使用，析构时删除
            auto ts = std::chrono::steady_clock::now().time_since_epoch().count(); // 代表从 steady_clock 纪元到现在的 时间计数值
            db_file_name_ = "bptree_mem_" + std::to_string(ts) + ".db";
            delete_db_on_destruct_ = true;

            disk_manager_ = std::make_unique<DiskManager>(db_file_name_);
            replacer_ = std::make_unique<LRUReplacer>(POOL_SIZE);
            bpm_ = std::make_unique<BufferPoolManager>(POOL_SIZE, disk_manager_.get(), replacer_.get());

            Page *meta_page = bpm_->FetchPage(0);
            root_page_id_ = *reinterpret_cast<page_id_t *>(meta_page->GetData());
            bpm_->UnpinPage(0, false);
            if (root_page_id_ == 0) {
                root_page_id_ = INVALID_PAGE_ID;
            }
        }

        // --- 公共方法 ---

        auto Is_Empty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }

        auto Insert(const KeyType &key, const ValueType &value) -> bool {
            if (Is_Empty()) {
                Start_New_Tree(key, value);
                return true;
            }

            std::vector<PageGuard> path;
            page_id_t current_page_id = root_page_id_;

            // Latch crabbing for insert: acquire W latch parent then child; if child safe, release all ancestors
            PageGuard current_guard = bpm_->FetchPageWriteGuard(current_page_id);
            while (true) {
                const char *parent_data = current_guard.GetData();
                NodeT node_view;

                // Push currently latched page
                path.push_back(std::move(current_guard));

                if (node_view.Is_Leaf(parent_data)) {
                    break;
                }

                InternalNodeT internal_view;
                page_id_t child_id = internal_view.Lookup(parent_data, internal_max_size_, key, comparator_);
                PageGuard child_guard = bpm_->FetchPageWriteGuard(child_id);

                // Check child safety for insert
                const char *child_data = child_guard.GetData();
                bool child_is_leaf = node_view.Is_Leaf(child_data);
                int child_size = node_view.Get_Size(child_data);
                bool child_safe = child_is_leaf ? (child_size < leaf_max_size_ - 1) : (child_size < internal_max_size_);

                if (child_safe) {
                    // Release all ancestors' latches to improve concurrency
                    for (auto &g : path) { g.Drop(); }
                    path.clear();
                }

                // Continue descending with child latched
                current_page_id = child_id;
                current_guard = std::move(child_guard);
            }

            PageGuard &leaf_guard = path.back();
            LeafNodeT leaf_view;
            int old_size = leaf_view.Get_Size(leaf_guard.GetData());
            leaf_view.Insert(leaf_guard.GetData(), leaf_max_size_, key, value, comparator_);
            int new_size = leaf_view.Get_Size(leaf_guard.GetData());

            if (new_size == old_size) {
                return false; // Duplicate key
            }
            leaf_guard.SetDirty();

            if (leaf_view.IS_Full(leaf_guard.GetData(), leaf_max_size_)) {
                Handle_Split(std::move(path));
            }
            return true;
        }

        auto Get_Value(const KeyType &key, ValueType *value) const -> bool {
            if (Is_Empty()) {
                return false;
            }

            auto leaf_guard = Find_Leaf_Guard(key);
            if (!leaf_guard) {
                return false;
            }

            LeafNodeT leaf_view;
            return leaf_view.Get_Value(leaf_guard.GetData(), leaf_max_size_, key, value, comparator_);
        }

        auto Begin() -> Iterator {
            if (this->Is_Empty()) {
                return this->End();
            }

            page_id_t current_page_id = root_page_id_;

            // --- [ 核心修正 ] ---
            // 使用 bpm_->FetchPageGuard() 来获取 PageGuard
            PageGuard guard = bpm_->FetchPageGuard(current_page_id);

            // 如果获取根页面失败，返回 End()
            if (!guard) {
                return this->End();
            }

            while (true) {
                const char *data = guard.GetData();
                NodeT node_view;
                if (node_view.Is_Leaf(data)) {
                    break; // 找到了最左边的叶子
                }

                InternalNodeT internal_view;
                // 注意：这里也需要修改，因为你的 internal_node.h 已经改了
                page_id_t child_page_id = internal_view.Child_At(data, internal_max_size_, 0);

                // 移动到子节点，移动赋值会自动处理旧 guard 的 unpin
                guard = bpm_->FetchPageGuard(child_page_id);

                if (!guard) {
                    // 如果在遍历过程中获取子页面失败，说明树结构可能存在问题
                    // 或者缓冲池已满且无法驱逐，返回 End() 是安全的做法
                    return this->End();
                }
            }

            // 创建一个指向第一个元素的迭代器
            return Iterator(bpm_.get(), guard.GetPageId(), 0, leaf_max_size_);
        }

        auto End() -> Iterator {
            // 返回一个无效的迭代器
            return Iterator(bpm_.get(), INVALID_PAGE_ID, 0, 0);
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

            // Descend with write latches; release ancestors when child safe
            std::vector<PageGuard> path;
            page_id_t current_page_id = root_page_id_;
            NodeT node_view;

            PageGuard current_guard = bpm_->FetchPageWriteGuard(current_page_id);
            while (true) {
                const char *parent_data = current_guard.GetData();
                path.push_back(std::move(current_guard));

                if (node_view.Is_Leaf(parent_data)) {
                    break;
                }

                InternalNodeT internal_view;
                page_id_t child_id = internal_view.Lookup(parent_data, internal_max_size_, key, comparator_);
                PageGuard child_guard = bpm_->FetchPageWriteGuard(child_id);

                const char *child_data = child_guard.GetData();
                bool child_is_leaf = node_view.Is_Leaf(child_data);
                int child_size = node_view.Get_Size(child_data);
                int min_size = child_is_leaf ? (leaf_max_size_ / 2) : ((internal_max_size_ + 1) / 2);
                bool child_safe = child_size > min_size;
                if (child_safe) {
                    for (auto &g : path) { g.Drop(); }
                    path.clear();
                }

                current_page_id = child_id;
                current_guard = std::move(child_guard);
            }

            // Delete at leaf
            PageGuard &leaf_guard = path.back();
            LeafNodeT leaf_view;
            char *leaf_data = leaf_guard.GetData();
            int old_size = leaf_view.Get_Size(leaf_data);
            leaf_view.Remove(leaf_data, leaf_max_size_, key, comparator_);
            int new_size = leaf_view.Get_Size(leaf_data);
            if (new_size == old_size) {
                return;
            }
            leaf_guard.SetDirty();

            // Underflow repair using locked path
            if (new_size < (leaf_max_size_ / 2) && path.size() >= 1) {
                Handle_Underflow_Locked(path);
            }
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

            if (Is_Empty()) {
                return result;
            }

            // Find starting leaf and index
            PageGuard leaf_guard = Find_Leaf_Guard(start_key);
            if (!leaf_guard) {
                return result;
            }
            LeafNodeT leaf_view;
            int index = leaf_view.Find_Key_Index(leaf_guard.GetData(), leaf_max_size_, start_key, comparator_);

            page_id_t page_id = leaf_guard.GetPageId();
            while (page_id != INVALID_PAGE_ID) {
                // Re-fetch current leaf safely (short critical section)
                PageGuard g = bpm_->FetchPageGuard(page_id);
                const char *data = g.GetData();
                int size = leaf_view.Get_Size(data);
                const KeyType *keys = leaf_view.Keys_Ptr(data);
                const ValueType *values = leaf_view.Values_Ptr(data, leaf_max_size_);

                for (int i = index; i < size; ++i) {
                    const KeyType &k = keys[i];
                    if (!comparator_(k, end_key)) {
                        return result;
                    }
                    result.emplace_back(k, values[i]);
                }
                // Move to next leaf
                page_id = leaf_view.Get_Next_Page_Id(data);
                index = 0;
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
            PageGuard leaf_guard = Find_Leaf_Guard(key);
            if (!leaf_guard) {
                return End();
            }
            LeafNodeT leaf_view;
            int index = leaf_view.Find_Key_Index(leaf_guard.GetData(), leaf_max_size_, key, comparator_);
            return Iterator(bpm_.get(), leaf_guard.GetPageId(), index, leaf_max_size_);
        }


    private:
        // --- 私有辅助函数 ---

        void Start_New_Tree(const KeyType &key, const ValueType &value) {
            page_id_t new_page_id;
            PageGuard root_guard = bpm_->NewPageGuard(&new_page_id);
            if (!root_guard) throw std::runtime_error("Failed to create new page for root.");

            root_page_id_ = new_page_id;

            LeafNodeT leaf_view;
            leaf_view.Init(root_guard.GetData(), leaf_max_size_);
            leaf_view.Insert(root_guard.GetData(), leaf_max_size_, key, value, comparator_);
            root_guard.SetDirty();
        };

        void Insert_Into_Parent(PageGuard &&child_guard, const KeyType &key, PageGuard &&sibling_guard);

        auto Find_Leaf_Guard(const KeyType &key) const -> PageGuard {
            page_id_t current_page_id = root_page_id_;
            NodeT node_view;
            PageGuard parent_guard = bpm_->FetchPageReadGuard(current_page_id);
            while (true) {
                const char *data = parent_guard.GetData();
                if (node_view.Is_Leaf(data)) return parent_guard;
                InternalNodeT internal_view;
                page_id_t child_id = internal_view.Lookup(data, internal_max_size_, key, comparator_);
                PageGuard child_guard = bpm_->FetchPageReadGuard(child_id);
                parent_guard.Drop();
                parent_guard = std::move(child_guard);
            }
        }

        void Handle_Split(std::vector<PageGuard> &&path) {
            PageGuard node_guard = std::move(path.back());
            path.pop_back();

            page_id_t new_sibling_id;
            PageGuard sibling_guard = bpm_->NewPageGuard(&new_sibling_id);
            if (!sibling_guard) throw std::runtime_error("Failed to create new page for sibling.");

            KeyType key_to_parent;
            NodeT node_view;

            if (node_view.Is_Leaf(node_guard.GetData())) {
                LeafNodeT leaf_view;
                leaf_view.Init(sibling_guard.GetData(), leaf_max_size_);
                key_to_parent = leaf_view.Split(node_guard.GetData(), sibling_guard.GetData(), leaf_max_size_);

                // Update sibling link
                leaf_view.Set_Next_Page_Id(sibling_guard.GetData(), leaf_view.Get_Next_Page_Id(node_guard.GetData()));
                leaf_view.Set_Next_Page_Id(node_guard.GetData(), new_sibling_id);
            } else {
                InternalNodeT internal_view;
                internal_view.Init(sibling_guard.GetData(), internal_max_size_);
                key_to_parent = internal_view.Split(node_guard.GetData(), sibling_guard.GetData(), internal_max_size_);
            }

            node_guard.SetDirty();
            sibling_guard.SetDirty();

            if (path.empty()) {
                // Root split
                page_id_t new_root_id;
                PageGuard new_root_guard = bpm_->NewPageGuard(&new_root_id);
                if (!new_root_guard) throw std::runtime_error("Failed to create new root page.");

                root_page_id_ = new_root_id;
                InternalNodeT root_view;
                root_view.Init(new_root_guard.GetData(), internal_max_size_);
                root_view.Populate_New_Root(new_root_guard.GetData(), internal_max_size_, key_to_parent,
                                            node_guard.GetPageId(), new_sibling_id);
                new_root_guard.SetDirty();
            } else {
                PageGuard &parent_guard = path.back();
                InternalNodeT parent_view;
                parent_view.Insert(parent_guard.GetData(), internal_max_size_, key_to_parent, new_sibling_id,
                                   comparator_);
                parent_guard.SetDirty();

                if (parent_view.IS_Full(parent_guard.GetData(), internal_max_size_)) {
                    Handle_Split(std::move(path)); // Recursive call with remaining path
                }
            }
        }


        /**
         * @brief 处理节点下溢（重分配或合并）
         */
        void Handle_Underflow(std::vector<page_id_t> &path) {
            page_id_t node_id = path.back();
            path.pop_back();

            // --- 1. 处理根节点下溢 ---
            if (node_id == root_page_id_) {
                // 使用正确的工厂方法获取Guard
                PageGuard root_guard = bpm_->FetchPageGuard(root_page_id_);
                if (!root_guard) return;

                NodeT node_view;
                const char *root_data = root_guard.GetData();

                // 情况1: 根是内部节点且变空 -> 降低树高
                if (!node_view.Is_Leaf(root_data) && node_view.Get_Size(root_data) == 0) {
                    InternalNodeT internal_view;
                    root_page_id_ = internal_view.Move_First_Child(root_guard.GetData(), internal_max_size_);
                    // 此时旧的根页面可以删除了
                    bpm_->DeletePage(node_id);
                }
                    // 情况2: 根是叶子节点且变空 -> 树变空
                else if (node_view.Is_Leaf(root_data) && node_view.Get_Size(root_data) == 0) {
                    root_page_id_ = INVALID_PAGE_ID;
                    bpm_->DeletePage(node_id);
                }
                return;
            }

            // --- 2. 获取父节点和兄弟节点信息 ---
            page_id_t parent_id = path.back();
            PageGuard parent_guard = bpm_->FetchPageGuard(parent_id);
            if (!parent_guard) return; // 获取父页面失败

            InternalNodeT parent_view;
            char *parent_data = parent_guard.GetData();
            int node_index = parent_view.Find_Child_Index(parent_data, internal_max_size_, node_id);

            // --- 3. 尝试从左兄弟借用 ---
            if (node_index > 0) {
                page_id_t left_sibling_id = parent_view.Child_At(parent_data, internal_max_size_, node_index - 1);
                PageGuard left_sibling_guard = bpm_->FetchPageGuard(left_sibling_id);
                if (!left_sibling_guard) return;

                NodeT sibling_view;
                if (sibling_view.Get_Size(left_sibling_guard.GetData()) >
                    sibling_view.Get_Min_Size(internal_max_size_)) {
                    PageGuard node_guard = bpm_->FetchPageGuard(node_id);
                    if (!node_guard) return;

                    int parent_key_index = node_index - 1;
                    NodeT node_view;
                    if (node_view.Is_Leaf(node_guard.GetData())) {
                        LeafNodeT leaf_view;
                        leaf_view.Move_Last_From(node_guard.GetData(), left_sibling_guard.GetData(), leaf_max_size_);
                        parent_view.Set_Key_At(parent_data, parent_key_index,
                                               leaf_view.Keys_Ptr(node_guard.GetData())[0]);
                    } else {
                        InternalNodeT internal_view;
                        internal_view.Move_Last_From(node_guard.GetData(), left_sibling_guard.GetData(),
                                                     internal_max_size_, parent_data, parent_key_index);
                    }

                    node_guard.SetDirty();
                    left_sibling_guard.SetDirty();
                    parent_guard.SetDirty();
                    return;
                }
            }

            // --- 4. 尝试从右兄弟借用 ---
            if (node_index < parent_view.Get_Size(parent_data)) {
                page_id_t right_sibling_id = parent_view.Child_At(parent_data, internal_max_size_, node_index + 1);
                PageGuard right_sibling_guard = bpm_->FetchPageGuard(right_sibling_id);
                if (!right_sibling_guard) return;

                NodeT sibling_view;
                if (sibling_view.Get_Size(right_sibling_guard.GetData()) >
                    sibling_view.Get_Min_Size(internal_max_size_)) {
                    PageGuard node_guard = bpm_->FetchPageGuard(node_id);
                    if (!node_guard) return;

                    int parent_key_index = node_index;
                    NodeT node_view;
                    if (node_view.Is_Leaf(node_guard.GetData())) {
                        LeafNodeT leaf_view;
                        leaf_view.Move_First_From(node_guard.GetData(), right_sibling_guard.GetData(), leaf_max_size_);
                        parent_view.Set_Key_At(parent_data, parent_key_index,
                                               leaf_view.Keys_Ptr(right_sibling_guard.GetData())[0]);
                    } else {
                        InternalNodeT internal_view;
                        internal_view.Move_First_From(node_guard.GetData(), right_sibling_guard.GetData(),
                                                      internal_max_size_, parent_data, parent_key_index);
                    }

                    node_guard.SetDirty();
                    right_sibling_guard.SetDirty();
                    parent_guard.SetDirty();
                    return;
                }
            }

            // --- 5. 执行合并 ---
            if (node_index > 0) {
                // 与左兄弟合并 (将 node 合并到 left_sibling)
                page_id_t left_sibling_id = parent_view.Child_At(parent_data, internal_max_size_, node_index - 1);
                PageGuard left_sibling_guard = bpm_->FetchPageGuard(left_sibling_id);
                PageGuard node_guard = bpm_->FetchPageGuard(node_id);
                if (!left_sibling_guard || !node_guard) return;

                int parent_key_index = node_index - 1;
                NodeT node_view;
                if (node_view.Is_Leaf(node_guard.GetData())) {
                    LeafNodeT leaf_view;
                    leaf_view.Merge(left_sibling_guard.GetData(), node_guard.GetData(), leaf_max_size_);
                } else {
                    InternalNodeT internal_view;
                    internal_view.Merge_Into(left_sibling_guard.GetData(), node_guard.GetData(), internal_max_size_,
                                             parent_data, parent_key_index);
                }

                parent_view.Remove_At(parent_data, internal_max_size_, parent_key_index);
                bpm_->DeletePage(node_id);
                left_sibling_guard.SetDirty();
                parent_guard.SetDirty();
            } else {
                // 与右兄弟合并 (将 right_sibling 合并到 node)
                page_id_t right_sibling_id = parent_view.Child_At(parent_data, internal_max_size_, node_index + 1);
                PageGuard right_sibling_guard = bpm_->FetchPageGuard(right_sibling_id);
                PageGuard node_guard = bpm_->FetchPageGuard(node_id);
                if (!right_sibling_guard || !node_guard) return;

                int parent_key_index = node_index;
                NodeT node_view;
                if (node_view.Is_Leaf(node_guard.GetData())) {
                    LeafNodeT leaf_view;
                    leaf_view.Merge(node_guard.GetData(), right_sibling_guard.GetData(), leaf_max_size_);
                } else {
                    InternalNodeT internal_view;
                    internal_view.Merge_Into(node_guard.GetData(), right_sibling_guard.GetData(), internal_max_size_,
                                             parent_data, parent_key_index);
                }

                parent_view.Remove_At(parent_data, internal_max_size_, parent_key_index);
                bpm_->DeletePage(right_sibling_id);
                node_guard.SetDirty();
                parent_guard.SetDirty();
            }

            // --- 6. 递归检查父节点 ---
            if (parent_view.Is_Underflow(parent_data, internal_max_size_)) {
                this->Handle_Underflow(path);
            }
        }

        // Underflow handler operating on held write-latched path; siblings are temporarily latched
        void Handle_Underflow_Locked(std::vector<PageGuard> &path) {
            // Current node is at path.back()
            PageGuard &node_guard = path.back();
            page_id_t node_id = node_guard.GetPageId();
            NodeT node_view;

            // Root handling
            if (node_id == root_page_id_) {
                const char *root_data = node_guard.GetData();
                if (!node_view.Is_Leaf(root_data) && node_view.Get_Size(root_data) == 0) {
                    InternalNodeT internal_view;
                    root_page_id_ = internal_view.Move_First_Child(node_guard.GetData(), internal_max_size_);
                } else if (node_view.Is_Leaf(root_data) && node_view.Get_Size(root_data) == 0) {
                    root_page_id_ = INVALID_PAGE_ID;
                }
                return;
            }

            // parent is previous in path
            PageGuard &parent_guard = path[path.size() - 2];
            InternalNodeT parent_view;
            char *parent_data = parent_guard.GetData();
            int node_index = parent_view.Find_Child_Index(parent_data, internal_max_size_, node_id);

            // Try borrow from left sibling
            if (node_index > 0) {
                page_id_t left_id = parent_view.Child_At(parent_data, internal_max_size_, node_index - 1);
                PageGuard left_guard = bpm_->FetchPageWriteGuard(left_id);
                NodeT sibling_view;
                if (sibling_view.Get_Size(left_guard.GetData()) > sibling_view.Get_Min_Size(internal_max_size_)) {
                    int parent_key_index = node_index - 1;
                    PageGuard &node_w = path.back();
                    if (node_view.Is_Leaf(node_w.GetData())) {
                        LeafNodeT leaf_view;
                        leaf_view.Move_Last_From(node_w.GetData(), left_guard.GetData(), leaf_max_size_);
                        parent_view.Set_Key_At(parent_data, parent_key_index,
                                               leaf_view.Keys_Ptr(node_w.GetData())[0]);
                    } else {
                        InternalNodeT internal_view;
                        internal_view.Move_Last_From(node_w.GetData(), left_guard.GetData(), internal_max_size_, parent_data,
                                                     parent_key_index);
                    }
                    node_w.SetDirty();
                    left_guard.SetDirty();
                    parent_guard.SetDirty();
                    return;
                }
            }

            // Try borrow from right sibling
            if (parent_view.Get_Size(parent_data) > node_index) {
                page_id_t right_id = parent_view.Child_At(parent_data, internal_max_size_, node_index + 1);
                PageGuard right_guard = bpm_->FetchPageWriteGuard(right_id);
                NodeT sibling_view;
                if (sibling_view.Get_Size(right_guard.GetData()) > sibling_view.Get_Min_Size(internal_max_size_)) {
                    int parent_key_index = node_index;
                    PageGuard &node_w = path.back();
                    if (node_view.Is_Leaf(node_w.GetData())) {
                        LeafNodeT leaf_view;
                        leaf_view.Move_First_From(node_w.GetData(), right_guard.GetData(), leaf_max_size_);
                        parent_view.Set_Key_At(parent_data, parent_key_index,
                                               leaf_view.Keys_Ptr(right_guard.GetData())[0]);
                    } else {
                        InternalNodeT internal_view;
                        internal_view.Move_First_From(node_w.GetData(), right_guard.GetData(), internal_max_size_, parent_data,
                                                      parent_key_index);
                    }
                    node_w.SetDirty();
                    right_guard.SetDirty();
                    parent_guard.SetDirty();
                    return;
                }
            }

            // Merge
            if (node_index > 0) {
                page_id_t left_id = parent_view.Child_At(parent_data, internal_max_size_, node_index - 1);
                PageGuard left_guard = bpm_->FetchPageWriteGuard(left_id);
                PageGuard &node_w = path.back();
                int parent_key_index = node_index - 1;
                if (node_view.Is_Leaf(node_w.GetData())) {
                    LeafNodeT leaf_view;
                    leaf_view.Merge(left_guard.GetData(), node_w.GetData(), leaf_max_size_);
                } else {
                    InternalNodeT internal_view;
                    internal_view.Merge_Into(left_guard.GetData(), node_w.GetData(), internal_max_size_, parent_data,
                                             parent_key_index);
                }
                parent_view.Remove_At(parent_data, internal_max_size_, parent_key_index);
                left_guard.SetDirty();
                parent_guard.SetDirty();
                // Replace node at path.back() with left sibling for potential upward fixes
                path.pop_back();
                path.back().SetDirty();
            } else {
                page_id_t right_id = parent_view.Child_At(parent_data, internal_max_size_, node_index + 1);
                PageGuard right_guard = bpm_->FetchPageWriteGuard(right_id);
                PageGuard &node_w = path.back();
                int parent_key_index = node_index;
                if (node_view.Is_Leaf(node_w.GetData())) {
                    LeafNodeT leaf_view;
                    leaf_view.Merge(node_w.GetData(), right_guard.GetData(), leaf_max_size_);
                } else {
                    InternalNodeT internal_view;
                    internal_view.Merge_Into(node_w.GetData(), right_guard.GetData(), internal_max_size_, parent_data,
                                             parent_key_index);
                }
                parent_view.Remove_At(parent_data, internal_max_size_, parent_key_index);
                node_w.SetDirty();
                parent_guard.SetDirty();
                // keep node as merged result
            }

            // Recurse upward if parent underflows
            if (parent_view.Is_Underflow(parent_data, internal_max_size_) && path.size() > 1) {
                // parent is now the back; call recursively
                PageGuard moved = std::move(path[path.size() - 2]);
                path.erase(path.end() - 2);
                path.push_back(std::move(moved));
                Handle_Underflow_Locked(path);
            }
        }

        // --- 成员变量 ---
        // B+Tree 的逻辑属性
        int leaf_max_size_;
        int internal_max_size_;
        KeyComparator comparator_;

        // 指向持久化层组件的智能指针
        std::unique_ptr<DiskManager> disk_manager_;
        std::unique_ptr<Replacer> replacer_;
        std::unique_ptr<BufferPoolManager> bpm_;

        // 指向树根节点的页面ID
        page_id_t root_page_id_;

        // 构造/析构辅助
        std::string db_file_name_;
        bool delete_db_on_destruct_{false};
    };
} // namespace bptree