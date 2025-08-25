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

            std::vector<PageGuard> path; // keep all ancestors to allow split propagation

            // Latch crabbing (write): take write latches top-down; keep them (conservative)
            PageGuard guard = bpm_->FetchPageWriteGuard(root_page_id_);
            NodeT node_view;
            while (!node_view.Is_Leaf(guard.GetData())) {
                // push current internal node onto path
                path.push_back(PageGuard(std::move(guard)));
                InternalNodeT internal_view;
                page_id_t child_id = internal_view.Lookup(path.back().GetData(), internal_max_size_, key, comparator_);
                guard = bpm_->FetchPageWriteGuard(child_id);
            }

            // At leaf
            LeafNodeT leaf_view;
            int old_size = leaf_view.Get_Size(guard.GetData());
            leaf_view.Insert(guard.GetData(), leaf_max_size_, key, value, comparator_);
            int new_size = leaf_view.Get_Size(guard.GetData());

            if (new_size == old_size) {
                return false; // Duplicate key
            }
            guard.SetDirty();

            // If overflow, need to split; include leaf into path vector tail
            if (leaf_view.IS_Full(guard.GetData(), leaf_max_size_)) {
                path.push_back(std::move(guard));
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
            PageGuard guard = bpm_->FetchPageReadGuard(current_page_id);

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
                guard = bpm_->FetchPageReadGuard(child_page_id);

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
            if (this->Is_Empty()) { return; }

            // Latch crabbing for delete: hold parent until child is safe (size > min)
            std::vector<PageGuard> path; // holds write-latched ancestors that are not safe

            PageGuard guard = bpm_->FetchPageWriteGuard(root_page_id_);
            NodeT node_view;
            while (!node_view.Is_Leaf(guard.GetData())) {
                InternalNodeT internal_view;
                page_id_t child_id = internal_view.Lookup(guard.GetData(), internal_max_size_, key, comparator_);
                PageGuard child_guard = bpm_->FetchPageWriteGuard(child_id);

                // Determine child safety for delete underflow
                bool child_is_leaf = node_view.Is_Leaf(child_guard.GetData());
                int max_for_child = child_is_leaf ? leaf_max_size_ : internal_max_size_;
                bool child_safe = node_view.Get_Size(child_guard.GetData()) > node_view.Get_Min_Size(max_for_child);

                if (!child_safe) { path.push_back(std::move(guard)); }
                guard = std::move(child_guard);
            }

            // Now at leaf with write latch
            LeafNodeT leaf_view;
            char *leaf_data = guard.GetData();
            int old_size = leaf_view.Get_Size(leaf_data);
            leaf_view.Remove(leaf_data, leaf_max_size_, key, comparator_);
            int new_size = leaf_view.Get_Size(leaf_data);

            if (new_size == old_size) { return; }
            guard.SetDirty();

            if (leaf_view.Is_Underflow(leaf_data, leaf_max_size_)) {
                // include node itself for underflow handling
                path.push_back(std::move(guard));
                this->Handle_Underflow(path);
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
            PageGuard root_guard = bpm_->NewPageWriteGuard(&new_page_id);
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
            PageGuard guard = bpm_->FetchPageReadGuard(current_page_id);
            NodeT node_view;
            while (!node_view.Is_Leaf(guard.GetData())) {
                InternalNodeT internal_view;
                page_id_t child_page_id = internal_view.Lookup(guard.GetData(), internal_max_size_, key, comparator_);
                // Latch child first (read), then release parent via move-assign
                PageGuard child_guard = bpm_->FetchPageReadGuard(child_page_id);
                guard = std::move(child_guard);
            }
            return guard;
        }

        void Handle_Split(std::vector<PageGuard> &&path) {
            PageGuard node_guard = std::move(path.back());
            path.pop_back();

            page_id_t new_sibling_id;
            PageGuard sibling_guard = bpm_->NewPageWriteGuard(&new_sibling_id);
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
                PageGuard new_root_guard = bpm_->NewPageWriteGuard(&new_root_id);
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
                char *parent_data = parent_guard.GetData();
                int child_index = parent_view.Find_Child_Index(parent_data, internal_max_size_, node_guard.GetPageId());
                parent_view.Insert_After_Child(parent_data, internal_max_size_, child_index, key_to_parent, new_sibling_id);
                parent_guard.SetDirty();

                if (parent_view.IS_Full(parent_data, internal_max_size_)) {
                    Handle_Split(std::move(path)); // Recursive call with remaining path
                }
            }
        }


        /**
         * @brief 处理节点下溢（重分配或合并）
         */
        void Handle_Underflow(std::vector<PageGuard> &path) {
            // node_guard is the underflowed node
            PageGuard node_guard = std::move(path.back());
            path.pop_back();

            page_id_t node_id = node_guard.GetPageId();

            // 1) Root underflow
            if (node_id == root_page_id_) {
                NodeT node_view;
                const char *root_data = node_guard.GetData();
                if (!node_view.Is_Leaf(root_data) && node_view.Get_Size(root_data) == 0) {
                    InternalNodeT internal_view;
                    root_page_id_ = internal_view.Move_First_Child(node_guard.GetData(), internal_max_size_);
                    page_id_t old_root = node_id;
                    // release guard before deletion
                    node_guard = PageGuard(nullptr, nullptr);
                    bpm_->DeletePage(old_root);
                } else if (node_view.Is_Leaf(root_data) && node_view.Get_Size(root_data) == 0) {
                    page_id_t old_root = node_id;
                    root_page_id_ = INVALID_PAGE_ID;
                    node_guard = PageGuard(nullptr, nullptr);
                    bpm_->DeletePage(old_root);
                }
                return;
            }

            // 2) Parent and siblings
            PageGuard &parent_guard = path.back();
            InternalNodeT parent_view;
            char *parent_data = parent_guard.GetData();
            int node_index = parent_view.Find_Child_Index(parent_data, internal_max_size_, node_id);

            // 3) Try borrow from left sibling
            if (node_index > 0) {
                page_id_t left_sibling_id = parent_view.Child_At(parent_data, internal_max_size_, node_index - 1);
                PageGuard left_sibling_guard = bpm_->FetchPageWriteGuard(left_sibling_id);
                NodeT sibling_view;
                if (sibling_view.Get_Size(left_sibling_guard.GetData()) > sibling_view.Get_Min_Size(internal_max_size_)) {
                    int parent_key_index = node_index - 1;
                    NodeT nview;
                    if (nview.Is_Leaf(node_guard.GetData())) {
                        LeafNodeT leaf_view;
                        leaf_view.Move_Last_From(node_guard.GetData(), left_sibling_guard.GetData(), leaf_max_size_);
                        parent_view.Set_Key_At(parent_data, parent_key_index, leaf_view.Keys_Ptr(node_guard.GetData())[0]);
                    } else {
                        InternalNodeT internal_view;
                        internal_view.Move_Last_From(node_guard.GetData(), left_sibling_guard.GetData(), internal_max_size_, parent_data, parent_key_index);
                    }
                    node_guard.SetDirty();
                    left_sibling_guard.SetDirty();
                    parent_guard.SetDirty();
                    return;
                }
            }

            // 4) Try borrow from right sibling
            if (node_index < parent_view.Get_Size(parent_data)) {
                page_id_t right_sibling_id = parent_view.Child_At(parent_data, internal_max_size_, node_index + 1);
                PageGuard right_sibling_guard = bpm_->FetchPageWriteGuard(right_sibling_id);
                NodeT sibling_view;
                if (sibling_view.Get_Size(right_sibling_guard.GetData()) > sibling_view.Get_Min_Size(internal_max_size_)) {
                    int parent_key_index = node_index;
                    NodeT nview;
                    if (nview.Is_Leaf(node_guard.GetData())) {
                        LeafNodeT leaf_view;
                        leaf_view.Move_First_From(node_guard.GetData(), right_sibling_guard.GetData(), leaf_max_size_);
                        parent_view.Set_Key_At(parent_data, parent_key_index, leaf_view.Keys_Ptr(right_sibling_guard.GetData())[0]);
                    } else {
                        InternalNodeT internal_view;
                        internal_view.Move_First_From(node_guard.GetData(), right_sibling_guard.GetData(), internal_max_size_, parent_data, parent_key_index);
                    }
                    node_guard.SetDirty();
                    right_sibling_guard.SetDirty();
                    parent_guard.SetDirty();
                    return;
                }
            }

            // 5) Merge
            if (node_index > 0) {
                // merge node into left sibling
                page_id_t left_sibling_id = parent_view.Child_At(parent_data, internal_max_size_, node_index - 1);
                PageGuard left_sibling_guard = bpm_->FetchPageWriteGuard(left_sibling_id);

                int parent_key_index = node_index - 1;
                NodeT nview;
                if (nview.Is_Leaf(node_guard.GetData())) {
                    LeafNodeT leaf_view;
                    leaf_view.Merge(left_sibling_guard.GetData(), node_guard.GetData(), leaf_max_size_);
                } else {
                    InternalNodeT internal_view;
                    internal_view.Merge_Into(left_sibling_guard.GetData(), node_guard.GetData(), internal_max_size_, parent_data, parent_key_index);
                }
                parent_view.Remove_At(parent_data, internal_max_size_, parent_key_index);
                page_id_t to_delete = node_guard.GetPageId();
                node_guard = PageGuard(nullptr, nullptr);
                bpm_->DeletePage(to_delete);
                left_sibling_guard.SetDirty();
                parent_guard.SetDirty();
            } else {
                // merge right sibling into node
                page_id_t right_sibling_id = parent_view.Child_At(parent_data, internal_max_size_, node_index + 1);
                PageGuard right_sibling_guard = bpm_->FetchPageWriteGuard(right_sibling_id);

                int parent_key_index = node_index;
                NodeT nview;
                if (nview.Is_Leaf(node_guard.GetData())) {
                    LeafNodeT leaf_view;
                    leaf_view.Merge(node_guard.GetData(), right_sibling_guard.GetData(), leaf_max_size_);
                } else {
                    InternalNodeT internal_view;
                    internal_view.Merge_Into(node_guard.GetData(), right_sibling_guard.GetData(), internal_max_size_, parent_data, parent_key_index);
                }
                parent_view.Remove_At(parent_data, internal_max_size_, parent_key_index);
                page_id_t to_delete = right_sibling_id;
                right_sibling_guard = PageGuard(nullptr, nullptr);
                bpm_->DeletePage(to_delete);
                node_guard.SetDirty();
                parent_guard.SetDirty();
            }

            // 6) Recurse on parent if needed
            if (parent_view.Is_Underflow(parent_data, internal_max_size_)) {
                this->Handle_Underflow(path);
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