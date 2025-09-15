#pragma once

#include <functional>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>
#include <string>
#include <chrono>
#include <mutex>
#include <shared_mutex>

#include "internal_node.h"
#include "leaf_node.h"
#include "node.h"
#include "config.h"
#include "buffer_pool_manager.h"
#include "page_guard.h"
#include "disk_manager.h"
#include "lru_replacer.h"
#include "transaction.h"

namespace bptree {

    static std::chrono::high_resolution_clock::time_point g_tree_construction_start;
    static std::chrono::milliseconds g_total_tree_lifetime{0};

    enum class Operation {
        Read, Insert, Remove
    };

    template<typename KeyT, typename ValueT, typename KeyComparator = std::less<KeyT>>
    class BPlusTree {
    public:
        using KeyType = KeyT;
        using ValueType = ValueT;
        using LeafNodeT = LeafNode<KeyT, ValueT, KeyComparator>;
        using InternalNodeT = InternalNode<KeyT, ValueT, KeyComparator>;
        using NodeT = Node<KeyT, ValueT, KeyComparator>;

        class BPlusTreeIterator {
        public:
            using value_type = std::pair<const KeyType, ValueType>;
            using pointer = const value_type *;

            BPlusTreeIterator() = default;

            ~BPlusTreeIterator() {
                if (bpm_ != nullptr && page_ != nullptr && page_id_ != INVALID_PAGE_ID) {
                    std::cout << "[ITER] dtor unlatch+unpin page=" << page_id_ << std::endl;
                    page_->RUnlatch();
                    bpm_->UnpinPage(page_id_, false);
                }
            }

            auto operator*() const -> value_type {
                LeafNodeT leaf_view;
                return {leaf_view.Keys_Ptr(page_->GetData())[index_in_leaf_],
                        leaf_view.Values_Ptr(page_->GetData(), leaf_max_size_)[index_in_leaf_]};
            }

            auto operator->() const -> pointer = delete;

            auto operator++() -> BPlusTreeIterator & {
                if (bpm_ == nullptr || page_id_ == INVALID_PAGE_ID) return *this;
                LeafNodeT leaf_view;
                int size = leaf_view.Get_Size(page_->GetData());
                if (index_in_leaf_ < size - 1) {
                    index_in_leaf_++;
                    return *this;
                }
                page_id_t next_id = leaf_view.Get_Next_Page_Id(page_->GetData());
                if (next_id != INVALID_PAGE_ID) {
                    Page *next_page = bpm_->FetchPage(next_id);
                    if (!next_page->TryRLatch()) {
                        std::cout << "[ITER] try latch next page failed, stay at page=" << page_id_ << std::endl;
                        bpm_->UnpinPage(next_id, false);
                        return *this;
                    }
                    std::cout << "[ITER] advance from page=" << page_id_ << " to page=" << next_id << std::endl;
                    page_->RUnlatch();
                    bpm_->UnpinPage(page_id_, false);
                    page_ = next_page;
                    page_id_ = next_id;
                    index_in_leaf_ = 0;
                    return *this;
                }
                std::cout << "[ITER] reach end at page=" << page_id_ << std::endl;
                page_->RUnlatch();
                bpm_->UnpinPage(page_id_, false);
                if (next_id == INVALID_PAGE_ID) {
                    page_ = nullptr;
                    page_id_ = INVALID_PAGE_ID;
                    index_in_leaf_ = 0;
                    return *this;
                }
                return *this;
            }

            auto operator++(int) -> BPlusTreeIterator {
                BPlusTreeIterator temp = *this;
                ++(*this);
                return temp;
            }

            auto operator==(const BPlusTreeIterator &other) const -> bool {
                return bpm_ == other.bpm_ && page_id_ == other.page_id_ && index_in_leaf_ == other.index_in_leaf_;
            }

            auto operator!=(const BPlusTreeIterator &other) const -> bool { return !(*this == other); }

        private:
            friend class BPlusTree;

            BPlusTreeIterator(BufferPoolManager *bpm, Page *page, page_id_t page_id, int index, int leaf_max_size)
                    : bpm_(bpm), page_(page), page_id_(page_id), index_in_leaf_(index), leaf_max_size_(leaf_max_size) {}

            BufferPoolManager *bpm_{nullptr};
            Page *page_{nullptr};
            page_id_t page_id_{INVALID_PAGE_ID};
            int index_in_leaf_{0};
            int leaf_max_size_{0};
        };


        using Iterator = BPlusTreeIterator;

        explicit BPlusTree(const std::string &db_file, int leaf_max_size, int internal_max_size)
                : leaf_max_size_(leaf_max_size), internal_max_size_(internal_max_size), comparator_(),
                  db_file_name_(db_file), delete_db_on_destruct_(false) {

            g_tree_construction_start = std::chrono::high_resolution_clock::now();

            disk_manager_ = std::make_unique<DiskManager>(db_file);
            replacer_ = std::make_unique<LRUReplacer>(POOL_SIZE);
            bpm_ = std::make_unique<BufferPoolManager>(POOL_SIZE, disk_manager_.get(), replacer_.get());

            Page *meta_page = bpm_->FetchPage(0);
            root_page_id_ = *reinterpret_cast<page_id_t *>(meta_page->GetData());
            std::cout << "[DEBUG] 构造函数: 从元数据页面读取到 root_page_id_=" << root_page_id_ << std::endl;
            bpm_->UnpinPage(0, false);

            if (root_page_id_ == 0) {
                root_page_id_ = INVALID_PAGE_ID;
                std::cout << "[DEBUG] 构造函数: root_page_id_为0，设置为INVALID_PAGE_ID" << std::endl;
            }
        }

        ~BPlusTree() {
            std::chrono::high_resolution_clock::now();

            if (bpm_ != nullptr) {
                std::cout << "[DEBUG] 析构函数: 开始析构，root_page_id_=" << root_page_id_ << std::endl;

                auto flush_start = std::chrono::high_resolution_clock::now();

                bpm_->FlushAllPages();
                std::cout << "[DEBUG] 析构函数: 已刷新所有脏页面" << std::endl;

                Page *meta_page = bpm_->FetchPage(0);
                *reinterpret_cast<page_id_t *>(meta_page->GetData()) = root_page_id_;
                std::cout << "[DEBUG] 析构函数: 已将 root_page_id_=" << root_page_id_ << " 写入元数据页面" << std::endl;
                bpm_->UnpinPage(0, true);
                bpm_->FlushPage(0);
                std::cout << "[DEBUG] 析构函数: 元数据页面已刷新到磁盘" << std::endl;

                auto flush_end = std::chrono::high_resolution_clock::now();
                std::chrono::duration_cast<std::chrono::milliseconds>(flush_end - flush_start);

                auto destruction_end = std::chrono::high_resolution_clock::now();

                g_total_tree_lifetime = std::chrono::duration_cast<std::chrono::milliseconds>(
                        destruction_end - g_tree_construction_start);
                std::cout << "\n=== B+树生命周期时间统计 ===" << std::endl;
                std::cout << "总生命周期耗时: " << g_total_tree_lifetime.count() << " ms" << std::endl;
                std::cout << "================================\n" << std::endl;
            }

            if (delete_db_on_destruct_) {
                bpm_.reset();
                replacer_.reset();
                disk_manager_.reset();
                if (!db_file_name_.empty()) {
                    std::remove(db_file_name_.c_str());
                }
            }
        }

        explicit BPlusTree(int leaf_max_size, int internal_max_size)
                : leaf_max_size_(leaf_max_size), internal_max_size_(internal_max_size), comparator_() {
            g_tree_construction_start = std::chrono::high_resolution_clock::now();

            auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
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

        auto Is_Empty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }

        auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr) -> bool {
            root_latch_.lock_shared();
            if (Is_Empty()) {
                root_latch_.unlock_shared();
                root_latch_.lock();
                if (Is_Empty()) {
                    std::cout << "[INS] empty tree, create root for key=" << key << std::endl;
                    Start_New_Tree(key, value);
                    root_latch_.unlock();
                    return true;
                }
                root_latch_.unlock();
                root_latch_.lock_shared();
            }

            if (transaction == nullptr) {
                throw std::logic_error("Insert requires a non-null transaction");
            }
            transaction->GetPageSet().clear();
            transaction->GetDeletedPageSet().clear();

            Page *leaf = Get_Leaf_Page(key, transaction, Operation::Insert);
            std::cout << "[INS] got leaf page=" << leaf->GetPageId() << " for key=" << key << std::endl;
            LeafNodeT leaf_view;
            ValueType existing{};
            if (leaf_view.Get_Value(leaf->GetData(), leaf_max_size_, key, &existing, comparator_)) {
                std::cout << "[INS] duplicate key, abort key=" << key << std::endl;
                Release_W_Latches(transaction);
                return false;
            }

            if (!leaf_view.IS_Full(leaf->GetData(), leaf_max_size_)) {
                leaf_view.Insert(leaf->GetData(), leaf_max_size_, key, value, comparator_);
                std::cout << "[INS] inserted, size=" << leaf_view.Get_Size(leaf->GetData()) << "/" << leaf_max_size_
                          << std::endl;
                {
                    ValueType chk{};
                    bool ok = leaf_view.Get_Value(leaf->GetData(), leaf_max_size_, key, &chk, comparator_);
                    std::cout << "[INS][CHK] key=" << key << " ok=" << (ok ? 1 : 0) << " value=" << chk << std::endl;
                }
                std::cout << "[INS] leaf not full, done." << std::endl;
                Release_W_Latches(transaction);
                return true;
            }

            Release_W_Latches(transaction);
            Page *leaf2 = Get_Leaf_Page(key, transaction, Operation::Insert, false);
            LeafNodeT leaf_view2;
            std::vector<Page *> path;
            {
                auto &ps = transaction->GetPageSet();
                for (Page *p: ps) if (p != nullptr) path.push_back(p);
            }

            Page *current = leaf2;
            typename LeafNodeT::KeyType up_key;
            page_id_t new_child_id = INVALID_PAGE_ID;

            {
                page_id_t new_leaf_id;
                Page *new_leaf_page = bpm_->NewPage(&new_leaf_id);
                LeafNodeT new_leaf_view;
                new_leaf_view.Init(new_leaf_page->GetData(), leaf_max_size_);
                up_key = leaf_view2.Split(current->GetData(), new_leaf_page->GetData(), leaf_max_size_);
                page_id_t next_id = leaf_view2.Get_Next_Page_Id(current->GetData());
                new_leaf_view.Set_Next_Page_Id(new_leaf_page->GetData(), next_id);
                leaf_view2.Set_Next_Page_Id(current->GetData(), new_leaf_id);

                if (comparator_(key, up_key)) {
                    leaf_view2.Insert(current->GetData(), leaf_max_size_, key, value, comparator_);
                } else {
                    new_leaf_page->WLatch();
                    new_leaf_view.Insert(new_leaf_page->GetData(), leaf_max_size_, key, value, comparator_);
                    new_leaf_page->WUnlatch();
                }

                bpm_->UnpinPage(new_leaf_id, true);
                new_child_id = new_leaf_id;
                std::cout << "[INS] split leaf page=" << current->GetPageId() << " new_leaf=" << new_leaf_id
                          << " up_key set" << std::endl;
            }

            while (true) {
                int idx_in_path = -1;
                for (int i = static_cast<int>(path.size()) - 1; i >= 0; --i) {
                    if (path[i] == current) {
                        idx_in_path = i;
                        break;
                    }
                }
                if (idx_in_path <= 0) {
                    page_id_t new_root_id;
                    Page *new_root_page = bpm_->NewPage(&new_root_id);
                    InternalNodeT root_view;
                    root_view.Init(new_root_page->GetData(), internal_max_size_);
                    root_view.Populate_New_Root(new_root_page->GetData(), internal_max_size_, up_key,
                                                current->GetPageId(), new_child_id);

                    root_page_id_ = new_root_id;
                    bpm_->UnpinPage(new_root_id, true);
                    std::cout << "[INS] create new root=" << new_root_id << std::endl;
                    break;
                }

                Page *parent_page = path[idx_in_path - 1];
                InternalNodeT parent_view;
                parent_view.Insert(parent_page->GetData(), internal_max_size_, up_key, new_child_id, comparator_);
                std::cout << "[INS] insert up_key into parent page=" << parent_page->GetPageId() << std::endl;

                if (!parent_view.IS_Full(parent_page->GetData(), internal_max_size_)) {
                    break;
                }

                page_id_t new_internal_id;
                Page *new_internal_page = bpm_->NewPage(&new_internal_id);
                InternalNodeT new_internal_view;
                new_internal_view.Init(new_internal_page->GetData(), internal_max_size_);
                auto up_key_next = parent_view.Split(parent_page->GetData(), new_internal_page->GetData(),
                                                     internal_max_size_);
                bpm_->UnpinPage(new_internal_id, true);
                std::cout << "[INS] split internal parent page=" << parent_page->GetPageId() << " new_internal="
                          << new_internal_id << std::endl;

                current = parent_page;
                up_key = up_key_next;
                new_child_id = new_internal_id;
            }

            Release_W_Latches(transaction);
            std::cout << "[INS] release all write latches done." << std::endl;
            return true;
        }

        auto Get_Value(const KeyType &key, ValueType *value, Transaction *transaction = nullptr) const -> bool {
            root_latch_.lock_shared();
            if (Is_Empty()) {
                root_latch_.unlock_shared();
                return false;
            }
            Page *leaf = const_cast<BPlusTree *>(this)->Get_Leaf_Page(key, nullptr, Operation::Read);
            LeafNodeT leaf_view;
            bool found = leaf_view.Get_Value(leaf->GetData(), leaf_max_size_, key, value, comparator_);
            std::cout << "[GET] key=" << key << " leaf=" << leaf->GetPageId() << " found=" << (found ? 1 : 0)
                      << std::endl;
            if (found) {
                int idx_dbg = leaf_view.Find_Key_Index(leaf->GetData(), leaf_max_size_, key, comparator_);
                int sz_dbg = leaf_view.Get_Size(leaf->GetData());
                auto keys_ptr = leaf_view.Keys_Ptr(leaf->GetData());
                auto vals_ptr = leaf_view.Values_Ptr(leaf->GetData(), leaf_max_size_);
                std::cout << "[GET][DBG] idx=" << idx_dbg << " size=" << sz_dbg << " value=" << *value << " keys=";
                for (int i = 0; i < sz_dbg; i++) std::cout << (i == 0 ? "" : " ") << keys_ptr[i];
                std::cout << " values=";
                for (int i = 0; i < sz_dbg; i++) std::cout << (i == 0 ? "" : " ") << vals_ptr[i];
                std::cout << std::endl;
            }
            if (!found) {
                int sz_dbg = leaf_view.Get_Size(leaf->GetData());
                std::cout << "[GET][DBG] leaf=" << leaf->GetPageId() << " size=" << sz_dbg << " keys=";
                auto keys_ptr = leaf_view.Keys_Ptr(leaf->GetData());
                for (int i = 0; i < sz_dbg; i++) {
                    std::cout << (i == 0 ? "" : " ") << keys_ptr[i];
                }
                std::cout << std::endl;
            }
            leaf->RUnlatch();
            bpm_->UnpinPage(leaf->GetPageId(), false);
            return found;
        }

        auto Begin() -> Iterator {
            root_latch_.lock_shared();
            if (this->Is_Empty()) {
                root_latch_.unlock_shared();
                return this->End();
            }
            page_id_t child = root_page_id_;
            Page *prev = nullptr;
            while (true) {
                if (child == INVALID_PAGE_ID) {
                    root_latch_.unlock_shared();
                    throw std::runtime_error("Begin(): child is INVALID_PAGE_ID");
                }
                Page *page = bpm_->FetchPage(child);
                if (page == nullptr) {
                    root_latch_.unlock_shared();
                    throw std::runtime_error("Begin(): FetchPage returned nullptr");
                }
                page->RLatch();
                if (prev == nullptr) {
                    root_latch_.unlock_shared();
                } else {
                    prev->RUnlatch();
                    bpm_->UnpinPage(prev->GetPageId(), false);
                }
                NodeT node_view;
                if (node_view.Is_Leaf(page->GetData())) {
                    std::cout << "[BEGIN] reach leftmost leaf page=" << child << std::endl;
                    return Iterator(bpm_.get(), page, child, 0, leaf_max_size_);
                }
                InternalNodeT internal_view;
                child = internal_view.Child_At(page->GetData(), internal_max_size_, 0);
                std::cout << "[BEGIN] descend to child page=" << child << std::endl;
                prev = page;
            }
        }

        auto End() -> Iterator {
            return Iterator(bpm_.get(), nullptr, INVALID_PAGE_ID, 0, 0);
        }

        auto begin() -> Iterator {
            return this->Begin();
        }

        auto end() -> Iterator {
            return this->End();
        }

        void Remove(const KeyType &key, Transaction *transaction = nullptr) {
            root_latch_.lock_shared();
            if (Is_Empty()) {
                root_latch_.unlock_shared();
                return;
            }
            if (transaction == nullptr) {
                throw std::logic_error("Remove requires a non-null transaction");
            }
            transaction->GetPageSet().clear();
            transaction->GetDeletedPageSet().clear();
            Page *leaf = Get_Leaf_Page(key, transaction, Operation::Remove);
            std::cout << "[DEL] got leaf page=" << leaf->GetPageId() << " for key=" << key << std::endl;
            LeafNodeT leaf_view;
            char *leaf_data = leaf->GetData();
            int old_size = leaf_view.Get_Size(leaf_data);
            leaf_view.Remove(leaf_data, leaf_max_size_, key, comparator_);
            int new_size = leaf_view.Get_Size(leaf_data);
            std::cout << "[DEL] removed key, size " << old_size << " -> " << new_size << std::endl;

            if (new_size < old_size && leaf_view.Is_Underflow(leaf_data, leaf_max_size_)) {
                std::cout << "[DEL] underflow at leaf page=" << leaf->GetPageId() << std::endl;
                Handle_Underflow(leaf, transaction);
            }

            Release_W_Latches(transaction);
            std::cout << "[DEL] release all write latches done." << std::endl;
            auto &delset = transaction->GetDeletedPageSet();
            for (auto pid: delset) {
                bpm_->DeletePage(pid);
                std::cout << "[DEL] delete page id=" << pid << std::endl;
            }
            delset.clear();
        }

        auto
        Range_Scan(const KeyType &start_key, const KeyType &end_key) -> std::vector<std::pair<KeyType, ValueType>> {
            std::vector<std::pair<KeyType, ValueType>> result;
            Iterator it = this->Begin(start_key);
            for (; it != this->End(); ++it) {
                if (comparator_((*it).first, end_key)) {
                    result.push_back({(*it).first, (*it).second});
                } else {
                    break;
                }
            }
            return result;
        }

        auto Begin(const KeyType &key) -> Iterator {
            root_latch_.lock_shared();
            if (Is_Empty()) {
                root_latch_.unlock_shared();
                return End();
            }
            Page *leaf = Get_Leaf_Page(key, nullptr, Operation::Read);
            LeafNodeT leaf_view;
            int index = leaf_view.Find_Key_Index(leaf->GetData(), leaf_max_size_, key, comparator_);
            return Iterator(bpm_.get(), leaf, leaf->GetPageId(), index, leaf_max_size_);
        }

    private:
        auto Is_Safe_Page(const char *page_data, bool is_leaf, bool is_root, Operation op) -> bool {
            if (op == Operation::Read) return true;
            NodeT node_view;
            int size = node_view.Get_Size(page_data);
            if (op == Operation::Insert) {
                if (is_leaf) return size < leaf_max_size_ - 1;
                return size < internal_max_size_;
            }
            if (is_root) {
                if (is_leaf) return size > 1;
                return size > 2;
            }
            if (is_leaf) return size > (leaf_max_size_ / 2);
            return size > ((internal_max_size_ + 1) / 2);
        }

        void Release_W_Latches(Transaction *txn) {
            if (txn == nullptr) return;
            auto &ps = txn->GetPageSet();
            while (!ps.empty()) {
                Page *p = ps.front();
                ps.pop_front();
                if (p == nullptr) {
                    std::cout << "[REL] unlock root latch" << std::endl;
                    root_latch_.unlock();
                } else {
                    std::cout << "[REL] WUnlatch+Unpin page=" << p->GetPageId() << std::endl;
                    p->WUnlatch();
                    bpm_->UnpinPage(p->GetPageId(), true);
                }
            }
        }

        void Release_Siblings(Page *left, Page *right) {
            if (left) {
                std::cout << "[UF] release left sibling page=" << left->GetPageId() << std::endl;
                left->WUnlatch();
                bpm_->UnpinPage(left->GetPageId(), true);
            }
            if (right) {
                std::cout << "[UF] release right sibling page=" << right->GetPageId() << std::endl;
                right->WUnlatch();
                bpm_->UnpinPage(right->GetPageId(), true);
            }
        }

        void Handle_Underflow(Page *page, Transaction *txn) {
            NodeT node_view;
            char *page_data = page->GetData();
            bool is_leaf = node_view.Is_Leaf(page_data);
            std::cout << "[UF] handle underflow at page=" << page->GetPageId() << " leaf=" << (is_leaf ? 1 : 0)
                      << std::endl;

            if (page->GetPageId() == root_page_id_) {
                NodeT node_view;
                char *page_data = page->GetData();
                bool is_leaf = node_view.Is_Leaf(page_data);
                int size = node_view.Get_Size(page_data);

                if (is_leaf) {
                    if (size == 0) {
                        root_page_id_ = INVALID_PAGE_ID;
                        txn->AddToDeletedPageSet(page->GetPageId());
                        std::cout << "[UF] root leaf becomes empty, set tree empty and mark delete page="
                                  << page->GetPageId() << std::endl;
                    }
                    return;
                }

                if (!is_leaf && size == 0) {
                    InternalNodeT internal_view;
                    page_id_t new_root_id = internal_view.Child_At(page_data, internal_max_size_, 0);
                    root_page_id_ = new_root_id;
                    txn->AddToDeletedPageSet(page->GetPageId());
                    std::cout << "[UF] root internal has single child, new root=" << new_root_id << " delete old root="
                              << page->GetPageId() << std::endl;
                }
                return;
            }

            Page *parent_page = nullptr;
            {
                auto &ps = txn->GetPageSet();
                std::vector<Page *> path;
                path.reserve(ps.size());
                for (Page *p: ps) { path.push_back(p); }
                int cur_idx = -1;
                for (int i = static_cast<int>(path.size()) - 1; i >= 0; --i) {
                    if (path[i] == page) {
                        cur_idx = i;
                        break;
                    }
                }
                if (cur_idx == -1) {
                    throw std::logic_error("Current page not found in transaction path for underflow handling.");
                }
                for (int i = cur_idx - 1; i >= 0; --i) {
                    if (path[i] != nullptr) {
                        parent_page = path[i];
                        break;
                    }
                }
                if (parent_page == nullptr) {
                    throw std::logic_error("Parent page not found in transaction path for underflow handling.");
                }
            }

            InternalNodeT parent_view;
            char *parent_data = parent_page->GetData();
            int index_in_parent = parent_view.Find_Child_Index(parent_data, internal_max_size_, page->GetPageId());
            if (index_in_parent == -1) {
                throw std::logic_error("Current page id not found in parent's children.");
            }
            std::cout << "[UF] parent=" << parent_page->GetPageId() << " index_in_parent=" << index_in_parent
                      << std::endl;

            Page *left_page = nullptr;
            Page *right_page = nullptr;
            char *left_data = nullptr;
            char *right_data = nullptr;
            if (index_in_parent > 0) {
                page_id_t left_id = parent_view.Child_At(parent_data, internal_max_size_, index_in_parent - 1);
                left_page = bpm_->FetchPage(left_id);
                if (left_page) {
                    left_page->WLatch();
                    left_data = left_page->GetData();
                }
            }
            if (index_in_parent < parent_view.Get_Size(parent_data)) {
                page_id_t right_id = parent_view.Child_At(parent_data, internal_max_size_, index_in_parent + 1);
                right_page = bpm_->FetchPage(right_id);
                if (right_page) {
                    right_page->WLatch();
                    right_data = right_page->GetData();
                }
            }
            std::cout << "[UF] siblings L=" << (left_page ? left_page->GetPageId() : -1) << " R="
                      << (right_page ? right_page->GetPageId() : -1) << std::endl;

            bool borrowed = false;
            if (is_leaf) {
                LeafNodeT leaf_view;
                if (left_page) {
                    NodeT sibling_view;
                    if (sibling_view.Get_Size(left_data) > sibling_view.Get_Min_Size(leaf_max_size_)) {
                        leaf_view.Move_Last_From(page_data, left_data, leaf_max_size_);
                        typename InternalNodeT::KeyType new_sep = leaf_view.Keys_Ptr(page_data)[0];
                        parent_view.Set_Key_At(parent_data, index_in_parent - 1, new_sep);
                        borrowed = true;
                        std::cout << "[UF] borrowed from left sibling for leaf" << std::endl;
                    }
                }
                if (!borrowed && right_page) {
                    NodeT sibling_view;
                    if (sibling_view.Get_Size(right_data) > sibling_view.Get_Min_Size(leaf_max_size_)) {
                        leaf_view.Move_First_From(page_data, right_data, leaf_max_size_);
                        typename InternalNodeT::KeyType new_sep = leaf_view.Keys_Ptr(right_data)[0];
                        parent_view.Set_Key_At(parent_data, index_in_parent, new_sep);
                        borrowed = true;
                        std::cout << "[UF] borrowed from right sibling for leaf" << std::endl;
                    }
                }
            } else {
                InternalNodeT curr_view;
                if (left_page) {
                    NodeT sibling_view;
                    if (sibling_view.Get_Size(left_data) > sibling_view.Get_Min_Size(internal_max_size_)) {
                        curr_view.Move_Last_From(page_data, left_data, internal_max_size_, parent_data,
                                                 index_in_parent - 1);
                        borrowed = true;
                        std::cout << "[UF] borrowed from left sibling for internal" << std::endl;
                    }
                }
                if (!borrowed && right_page) {
                    NodeT sibling_view;
                    if (sibling_view.Get_Size(right_data) > sibling_view.Get_Min_Size(internal_max_size_)) {
                        curr_view.Move_First_From(page_data, right_data, internal_max_size_, parent_data,
                                                  index_in_parent);
                        borrowed = true;
                        std::cout << "[UF] borrowed from right sibling for internal" << std::endl;
                    }
                }
            }

            if (borrowed) {
                Release_Siblings(left_page, right_page);
                return;
            }

            if (is_leaf) {
                LeafNodeT leaf_view;
                if (left_page) {
                    leaf_view.Merge(left_data, page_data, leaf_max_size_);
                    parent_view.Remove_At(parent_data, internal_max_size_, index_in_parent - 1);
                    txn->AddToDeletedPageSet(page->GetPageId());
                    std::cout << "[UF] merged current leaf into left" << std::endl;
                } else if (right_page) {
                    leaf_view.Merge(page_data, right_data, leaf_max_size_);
                    parent_view.Remove_At(parent_data, internal_max_size_, index_in_parent);
                    txn->AddToDeletedPageSet(right_page->GetPageId());
                    std::cout << "[UF] merged right leaf into current" << std::endl;
                } else {
                    Release_Siblings(left_page, right_page);
                    throw std::logic_error("Leaf underflow with no siblings.");
                }
            } else {
                InternalNodeT curr_view;
                if (left_page) {
                    curr_view.Merge_Into(left_data, page_data, internal_max_size_, parent_data, index_in_parent - 1);
                    parent_view.Remove_At(parent_data, internal_max_size_, index_in_parent - 1);
                    txn->AddToDeletedPageSet(page->GetPageId());
                    std::cout << "[UF] merged current internal into left" << std::endl;
                } else if (right_page) {
                    curr_view.Merge_Into(page_data, right_data, internal_max_size_, parent_data, index_in_parent);
                    parent_view.Remove_At(parent_data, internal_max_size_, index_in_parent);
                    txn->AddToDeletedPageSet(right_page->GetPageId());
                    std::cout << "[UF] merged right internal into current" << std::endl;
                } else {
                    Release_Siblings(left_page, right_page);
                    throw std::logic_error("Internal underflow with no siblings.");
                }
            }

            Release_Siblings(left_page, right_page);

            NodeT parent_node_view;
            if (parent_node_view.Is_Underflow(parent_data, internal_max_size_)) {
                std::cout << "[UF] parent underflow, recurse parent page=" << parent_page->GetPageId() << std::endl;
                Handle_Underflow(parent_page, txn);
            }
        }

        auto Get_Leaf_Page(const KeyType &key, Transaction *txn, Operation op, bool first_round = true) -> Page * {
            if (!first_round) {
                root_latch_.lock();
                if (txn == nullptr) {
                    throw std::logic_error("Write operations require a non-null transaction in second round.");
                }
                txn->AddToPageSet(nullptr);
            }

            page_id_t child = root_page_id_;
            Page *prev = nullptr;

            while (true) {
                if (child == INVALID_PAGE_ID) {
                    throw std::runtime_error("Get_Leaf_Page: child is INVALID_PAGE_ID");
                }
                std::cout << "[GLP] fetch child=" << child << " first_round=" << (first_round ? 1 : 0) << " op="
                          << (int) op << std::endl;
                Page *page = bpm_->FetchPage(child);
                if (page == nullptr) {
                    throw std::runtime_error("Get_Leaf_Page: FetchPage returned nullptr");
                }
                const char *data = page->GetData();
                if (data == nullptr) {
                    bpm_->UnpinPage(page->GetPageId(), false);
                    throw std::runtime_error("Get_Leaf_Page: Page->GetData() is nullptr");
                }
                NodeT node_view;
                bool is_leaf = node_view.Is_Leaf(data);

                if (first_round) {
                    if (is_leaf && op != Operation::Read) {
                        page->WLatch();
                        if (txn == nullptr) {
                            throw std::logic_error("Insert/Remove requires a non-null transaction.");
                        }
                        txn->AddToPageSet(page);
                        std::cout << "[GLP] WLatch leaf page=" << page->GetPageId() << std::endl;
                    } else {
                        page->RLatch();
                        std::cout << "[GLP] RLatch page=" << page->GetPageId() << std::endl;
                    }
                    if (prev == nullptr) {
                        root_latch_.unlock_shared();
                    } else {
                        std::cout << "[GLP] release prev RLatch+Unpin page=" << prev->GetPageId() << std::endl;
                        prev->RUnlatch();
                        bpm_->UnpinPage(prev->GetPageId(), false);
                    }
                } else {
                    page->WLatch();
                    txn->AddToPageSet(page);
                    std::cout << "[GLP] WLatch page=" << page->GetPageId() << std::endl;
                }

                if (is_leaf) {
                    if (first_round && !Is_Safe_Page(data, true, child == root_page_id_, op)) {
                        std::cout << "[GLP] leaf not safe, restart second round" << std::endl;
                        Release_W_Latches(txn);
                        return Get_Leaf_Page(key, txn, op, false);
                    }
                    std::cout << "[GLP] return leaf page=" << page->GetPageId() << std::endl;
                    return page;
                }

                InternalNodeT internal_view;
                child = internal_view.Lookup(data, internal_max_size_, key, comparator_);
                if (child == INVALID_PAGE_ID) {
                    if (first_round) {
                        page->RUnlatch();
                        bpm_->UnpinPage(page->GetPageId(), false);
                    } else {
                        page->WUnlatch();
                        bpm_->UnpinPage(page->GetPageId(), true);
                    }
                    throw std::runtime_error("Get_Leaf_Page: Lookup returned INVALID_PAGE_ID");
                }
                prev = page;
            }
        }

        void Start_New_Tree(const KeyType &key, const ValueType &value) {
            page_id_t new_page_id;
            PageGuard root_guard = bpm_->NewPageGuard(&new_page_id);
            if (!root_guard) throw std::runtime_error("Failed to create new page for root.");

            if (new_page_id == 0) {
                bpm_->DeletePage(0);
                root_guard = bpm_->NewPageGuard(&new_page_id);
                if (!root_guard) throw std::runtime_error("Failed to create new page for root.");
            }

            root_page_id_ = new_page_id;
            std::cout << "[DEBUG] Start_New_Tree: 创建新根页面，page_id=" << new_page_id << std::endl;

            LeafNodeT leaf_view;
            leaf_view.Init(root_guard.GetData(), leaf_max_size_);
            leaf_view.Insert(root_guard.GetData(), leaf_max_size_, key, value, comparator_);
            root_guard.SetDirty();

            std::cout << "[DEBUG] Start_New_Tree: 插入数据后，叶子节点大小=" << leaf_view.Get_Size(root_guard.GetData())
                      << std::endl;
            std::cout << "[DEBUG] Start_New_Tree: 准备刷新页面到磁盘..." << std::endl;

            bpm_->FlushPage(new_page_id);

            std::cout << "[DEBUG] Start_New_Tree: 页面已刷新到磁盘" << std::endl;

            Page *meta_page = bpm_->FetchPage(0);
            *reinterpret_cast<page_id_t *>(meta_page->GetData()) = root_page_id_;
            bpm_->UnpinPage(0, true);
            bpm_->FlushPage(0);
            std::cout << "[DEBUG] Start_New_Tree: 元数据页面已更新，root_page_id_=" << root_page_id_ << std::endl;
        };

        auto Find_Parent_Page_Id(page_id_t child_id) const -> page_id_t {
            if (child_id == root_page_id_) {
                return INVALID_PAGE_ID;
            }

            return Find_Parent_Recursive(root_page_id_, child_id);
        }

        auto Find_Parent_Recursive(page_id_t current_id, page_id_t target_child_id) const -> page_id_t {
            if (current_id == INVALID_PAGE_ID) {
                return INVALID_PAGE_ID;
            }

            PageGuard guard = bpm_->FetchPageGuard(current_id);
            if (!guard) return INVALID_PAGE_ID;

            const char *data = guard.GetData();
            NodeT node_view;

            if (node_view.Is_Leaf(data)) {
                return INVALID_PAGE_ID;
            }

            InternalNodeT internal_view;
            int size = internal_view.Get_Size(data);

            for (int i = 0; i <= size; ++i) {
                page_id_t child_page_id = internal_view.Child_At(data, internal_max_size_, i);
                if (child_page_id == target_child_id) {
                    return current_id;
                }
            }

            static int recursion_depth = 0;
            if (recursion_depth > 100) {
                return INVALID_PAGE_ID;
            }

            recursion_depth++;
            for (int i = 0; i <= size; ++i) {
                page_id_t child_page_id = internal_view.Child_At(data, internal_max_size_, i);
                if (child_page_id != INVALID_PAGE_ID) {
                    page_id_t result = Find_Parent_Recursive(child_page_id, target_child_id);
                    if (result != INVALID_PAGE_ID) {
                        recursion_depth--;
                        return result;
                    }
                }
            }
            recursion_depth--;
            return INVALID_PAGE_ID;
        }

        void Handle_Split(PageGuard &&node_guard) {
            page_id_t new_sibling_id;
            PageGuard sibling_guard = bpm_->NewPageGuard(&new_sibling_id);
            if (!sibling_guard) throw std::runtime_error("Failed to create new page for sibling.");

            KeyType key_to_parent;
            NodeT node_view;

            if (node_view.Is_Leaf(node_guard.GetData())) {
                LeafNodeT leaf_view;
                leaf_view.Init(sibling_guard.GetData(), leaf_max_size_);
                key_to_parent = leaf_view.Split(node_guard.GetData(), sibling_guard.GetData(), leaf_max_size_);

                leaf_view.Set_Next_Page_Id(sibling_guard.GetData(), leaf_view.Get_Next_Page_Id(node_guard.GetData()));
                leaf_view.Set_Next_Page_Id(node_guard.GetData(), new_sibling_id);
            } else {
                InternalNodeT internal_view;
                internal_view.Init(sibling_guard.GetData(), internal_max_size_);
                key_to_parent = internal_view.Split(node_guard.GetData(), sibling_guard.GetData(), internal_max_size_);
            }

            node_guard.SetDirty();
            sibling_guard.SetDirty();

            if (node_guard.GetPageId() == root_page_id_) {
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
                page_id_t parent_id = Find_Parent_Page_Id(node_guard.GetPageId());
                if (parent_id != INVALID_PAGE_ID) {
                    PageGuard parent_guard = bpm_->FetchPageGuard(parent_id);
                    if (parent_guard) {
                        InternalNodeT parent_view;
                        parent_view.Insert(parent_guard.GetData(), internal_max_size_, key_to_parent, new_sibling_id,
                                           comparator_);
                        parent_guard.SetDirty();
                        if (parent_view.IS_Full(parent_guard.GetData(), internal_max_size_)) {
                            Handle_Split(std::move(parent_guard));
                        }
                    } else {
                    }
                } else {
                }
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
                    Handle_Split(std::move(path));
                }
            }
        }

        void Handle_Underflow(std::vector<page_id_t> &path) {
            page_id_t node_id = path.back();
            path.pop_back();

            if (node_id == root_page_id_) {
                PageGuard root_guard = bpm_->FetchPageGuard(root_page_id_);
                if (!root_guard) return;

                NodeT node_view;
                const char *root_data = root_guard.GetData();
                if (!node_view.Is_Leaf(root_data) && node_view.Get_Size(root_data) == 0) {
                    InternalNodeT internal_view;
                    root_page_id_ = internal_view.Move_First_Child(root_guard.GetData(), internal_max_size_);
                    bpm_->DeletePage(node_id);
                }
                else if (node_view.Is_Leaf(root_data) && node_view.Get_Size(root_data) == 0) {
                    root_page_id_ = INVALID_PAGE_ID;
                    bpm_->DeletePage(node_id);
                }
                return;
            }
            page_id_t parent_id = path.back();
            PageGuard parent_guard = bpm_->FetchPageGuard(parent_id);
            if (!parent_guard) return;

            InternalNodeT parent_view;
            char *parent_data = parent_guard.GetData();
            int node_index = parent_view.Find_Child_Index(parent_data, internal_max_size_, node_id);

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

            if (node_index > 0) {
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

            if (parent_view.Is_Underflow(parent_data, internal_max_size_)) {
                this->Handle_Underflow(path);
            }
        }
        void Handle_Underflow(PageGuard &&node_guard) {
            page_id_t node_id = node_guard.GetPageId();

            if (node_id == root_page_id_) {
                NodeT node_view;
                const char *node_data = node_guard.GetData();

                if (!node_view.Is_Leaf(node_data) && node_view.Get_Size(node_data) == 0) {
                    InternalNodeT internal_view;
                    root_page_id_ = internal_view.Move_First_Child(node_guard.GetData(), internal_max_size_);
                    bpm_->DeletePage(node_id);
                }
                else if (node_view.Is_Leaf(node_data) && node_view.Get_Size(node_data) == 0) {
                    root_page_id_ = INVALID_PAGE_ID;
                    bpm_->DeletePage(node_id);
                }
                return;
            }
        }
        int leaf_max_size_;
        int internal_max_size_;
        KeyComparator comparator_;

        std::unique_ptr<DiskManager> disk_manager_;
        std::unique_ptr<Replacer> replacer_;
        std::unique_ptr<BufferPoolManager> bpm_;

        page_id_t root_page_id_;

        std::string db_file_name_;
        bool delete_db_on_destruct_{false};

        mutable std::shared_mutex root_latch_;
    };
} // namespace bptree