#pragma once

#include <functional> // for std::less
#include <iterator>   // for std::iterator_traits
#include <memory>     // for std::unique_ptr, std::make_unique
#include <stdexcept>  // for std::runtime_error
#include <utility>    // for std::pair, std::move
#include <vector>
#include <string>
#include <chrono>
#include <mutex>      // for std::mutex
#include <shared_mutex> // for std::shared_mutex

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

    // 全局时间统计变量
    static std::chrono::high_resolution_clock::time_point g_tree_construction_start;
    static std::chrono::high_resolution_clock::time_point g_tree_destruction_start;
    static std::chrono::milliseconds g_total_tree_lifetime{0};
    static std::chrono::milliseconds g_construction_time{0};
    static std::chrono::milliseconds g_destruction_time{0};
    static std::chrono::milliseconds g_disk_flush_time{0};

    enum class Operation { Read, Insert, Remove };

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
                LeafNodeT leaf_view;
                int size = leaf_view.Get_Size(page_->GetData());
                if (index_in_leaf_ < size - 1) {
                index_in_leaf_++;
                    return *this;
                }
                // move to next leaf
                page_id_t next_id = leaf_view.Get_Next_Page_Id(page_->GetData());
                // try non-blocking latch on next page
                if (next_id != INVALID_PAGE_ID) {
                    Page *next_page = bpm_->FetchPage(next_id);
                    if (!next_page->TryRLatch()) {
                        // cannot advance now; do not block, keep iterator at current end position
                        std::cout << "[ITER] try latch next page failed, stay at page=" << page_id_ << std::endl;
                        bpm_->UnpinPage(next_id, false);
                        return *this;
                    }
                    // switch to next
                    std::cout << "[ITER] advance from page=" << page_id_ << " to page=" << next_id << std::endl;
                    page_->RUnlatch();
                    bpm_->UnpinPage(page_id_, false);
                    page_ = next_page;
                    page_id_ = next_id;
                    index_in_leaf_ = 0;
                    return *this;
                }
                // no next page: finalize iterator
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

            BPlusTreeIterator(BufferPoolManager *bpm, Page *page, page_id_t page_id, int index, int leaf_max_size)
                    : bpm_(bpm), page_(page), page_id_(page_id), index_in_leaf_(index), leaf_max_size_(leaf_max_size) {}

            BufferPoolManager *bpm_{nullptr};
            Page *page_{nullptr};
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

            // 记录构建开始时间
            g_tree_construction_start = std::chrono::high_resolution_clock::now();

            disk_manager_ = std::make_unique<DiskManager>(db_file);
            replacer_ = std::make_unique<LRUReplacer>(POOL_SIZE);
            bpm_ = std::make_unique<BufferPoolManager>(POOL_SIZE, disk_manager_.get(), replacer_.get());

            // 使用 page_id 0 作为元数据页
            Page *meta_page = bpm_->FetchPage(0);
            root_page_id_ = *reinterpret_cast<page_id_t *>(meta_page->GetData());
            std::cout << "[DEBUG] 构造函数: 从元数据页面读取到 root_page_id_=" << root_page_id_ << std::endl;
            bpm_->UnpinPage(0, false);

            if (root_page_id_ == 0) { // Uninitialized
                root_page_id_ = INVALID_PAGE_ID;
                std::cout << "[DEBUG] 构造函数: root_page_id_为0，设置为INVALID_PAGE_ID" << std::endl;
            }
        }

        ~BPlusTree() {
            // 记录析构开始时间
            g_tree_destruction_start = std::chrono::high_resolution_clock::now();
            
            if (bpm_ != nullptr) {
                std::cout << "[DEBUG] 析构函数: 开始析构，root_page_id_=" << root_page_id_ << std::endl;

                // 记录磁盘刷新开始时间
                auto flush_start = std::chrono::high_resolution_clock::now();
                
                // 首先刷新所有脏页面到磁盘
                bpm_->FlushAllPages();
                std::cout << "[DEBUG] 析构函数: 已刷新所有脏页面" << std::endl;

                // 然后更新并刷新元数据页面
                Page *meta_page = bpm_->FetchPage(0);
                *reinterpret_cast<page_id_t *>(meta_page->GetData()) = root_page_id_;
                std::cout << "[DEBUG] 析构函数: 已将 root_page_id_=" << root_page_id_ << " 写入元数据页面" << std::endl;
                bpm_->UnpinPage(0, true);
                // 确保元数据页面立即刷新到磁盘
                bpm_->FlushPage(0);
                std::cout << "[DEBUG] 析构函数: 元数据页面已刷新到磁盘" << std::endl;
                
                // 计算磁盘刷新耗时
                auto flush_end = std::chrono::high_resolution_clock::now();
                g_disk_flush_time = std::chrono::duration_cast<std::chrono::milliseconds>(flush_end - flush_start);
                
                // 计算总析构时间
                auto destruction_end = std::chrono::high_resolution_clock::now();
                
                // 计算总生命周期时间
                g_total_tree_lifetime = std::chrono::duration_cast<std::chrono::milliseconds>(destruction_end - g_tree_construction_start);
                // 输出时间统计信息
                std::cout << "\n=== B+树生命周期时间统计 ===" << std::endl;
                std::cout << "总生命周期耗时: " << g_total_tree_lifetime.count() << " ms" << std::endl;
                std::cout << "================================\n" << std::endl;
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
            // 记录构建开始时间
            g_tree_construction_start = std::chrono::high_resolution_clock::now();
            
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

        // 获取时间统计信息的静态方法
        static auto Get_Construction_Time() -> std::chrono::milliseconds { return g_construction_time; }
        static auto Get_Destruction_Time() -> std::chrono::milliseconds { return g_destruction_time; }
        static auto Get_Disk_Flush_Time() -> std::chrono::milliseconds { return g_disk_flush_time; }
        static auto Get_Total_Lifetime() -> std::chrono::milliseconds { return g_total_tree_lifetime; }
        
        // 打印时间统计信息
        static void Print_Time_Statistics() {
            std::cout << "\n=== B+树时间统计信息 ===" << std::endl;
            std::cout << "构建阶段耗时: " << g_construction_time.count() << " ms" << std::endl;
            std::cout << "析构阶段耗时: " << g_destruction_time.count() << " ms" << std::endl;
            std::cout << "磁盘刷新耗时: " << g_disk_flush_time.count() << " ms" << std::endl;
            std::cout << "总生命周期耗时: " << g_total_tree_lifetime.count() << " ms" << std::endl;
            std::cout << "==========================\n" << std::endl;
        }

        auto Is_Empty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }

        auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr) -> bool {
            root_latch_.lock_shared();
            // 空树：需要升级到写锁创建根
            if (Is_Empty()) {
                root_latch_.unlock_shared();
                root_latch_.lock();
                if (Is_Empty()) {
                    std::cout << "[INS] empty tree, create root for key=" << key << std::endl;
                Start_New_Tree(key, value);
                    root_latch_.unlock();
                return true;
                }
                // 降级为读锁继续
                root_latch_.unlock();
                root_latch_.lock_shared();
            }

            if (transaction == nullptr) {
                throw std::logic_error("Insert requires a non-null transaction");
            }
            // 清理上一次操作可能遗留的页面集合/删除集合，避免持锁/未清空导致的资源耗尽
            transaction->GetPageSet().clear();
            transaction->GetDeletedPageSet().clear();

            Page *leaf = Get_Leaf_Page(key, transaction, Operation::Insert);
            std::cout << "[INS] got leaf page=" << leaf->GetPageId() << " for key=" << key << std::endl;
            LeafNodeT leaf_view;
            ValueType existing{};
            if (leaf_view.Get_Value(leaf->GetData(), leaf_max_size_, key, &existing, comparator_)) {
                std::cout << "[INS] duplicate key, abort key=" << key << std::endl;
                Release_W_Latches(transaction);
                return false; // duplicate key
            }

            // 如果叶子未满，直接插入并返回
            if (!leaf_view.IS_Full(leaf->GetData(), leaf_max_size_)) {
                // 插入到叶子
                leaf_view.Insert(leaf->GetData(), leaf_max_size_, key, value, comparator_);
                std::cout << "[INS] inserted, size=" << leaf_view.Get_Size(leaf->GetData()) << "/" << leaf_max_size_ << std::endl;
                {
                    // verify written value immediately
                    ValueType chk{};
                    bool ok = leaf_view.Get_Value(leaf->GetData(), leaf_max_size_, key, &chk, comparator_);
                    std::cout << "[INS][CHK] key=" << key << " ok=" << (ok?1:0) << " value=" << chk << std::endl;
                }
                std::cout << "[INS] leaf not full, done." << std::endl;
                Release_W_Latches(transaction);
                return true;
            }

            // 叶满：释放第一轮路径，第二轮获取完整写锁路径再分裂，然后再插入
            Release_W_Latches(transaction);
            Page *leaf2 = Get_Leaf_Page(key, transaction, Operation::Insert, false);
            LeafNodeT leaf_view2;
            std::vector<Page *> path;
            {
                auto &ps = transaction->GetPageSet();
                for (Page *p : ps) if (p != nullptr) path.push_back(p);
            }

            Page *current = leaf2;
            // 将在循环内反复使用的变量
            typename LeafNodeT::KeyType up_key; // 提升到父节点的键
            page_id_t new_child_id = INVALID_PAGE_ID; // 右侧新兄弟页 id

            // 首先处理叶子分裂
            {
                page_id_t new_leaf_id;
                Page *new_leaf_page = bpm_->NewPage(&new_leaf_id);
                LeafNodeT new_leaf_view;
                new_leaf_view.Init(new_leaf_page->GetData(), leaf_max_size_);
                up_key = leaf_view2.Split(current->GetData(), new_leaf_page->GetData(), leaf_max_size_);
                // 维护叶子链表
                page_id_t next_id = leaf_view2.Get_Next_Page_Id(current->GetData());
                new_leaf_view.Set_Next_Page_Id(new_leaf_page->GetData(), next_id);
                leaf_view2.Set_Next_Page_Id(current->GetData(), new_leaf_id);

                // 分裂完成后，将 (key, value) 插入到正确的叶子（比较 key 与 up_key）
                if (comparator_(key, up_key)) {
                    // 插入左叶（current 已在事务中持有写锁）
                    leaf_view2.Insert(current->GetData(), leaf_max_size_, key, value, comparator_);
                } else {
                    // 插入右叶（临时写锁 new_leaf_page）
                    new_leaf_page->WLatch();
                    new_leaf_view.Insert(new_leaf_page->GetData(), leaf_max_size_, key, value, comparator_);
                    new_leaf_page->WUnlatch();
                }

                // 立即释放新页（不进入 transaction）
                bpm_->UnpinPage(new_leaf_id, true);
                new_child_id = new_leaf_id;
                std::cout << "[INS] split leaf page=" << current->GetPageId() << " new_leaf=" << new_leaf_id << " up_key set" << std::endl;
            }

            // 向上插入，必要时继续分裂
            while (true) {
                // 找到 current 的父页面：在 path 中 current 的前一个元素
                int idx_in_path = -1;
                for (int i = static_cast<int>(path.size()) - 1; i >= 0; --i) {
                    if (path[i] == current) { idx_in_path = i; break; }
                }
                if (idx_in_path <= 0) {
                    // 说明 current 是路径的首个节点（可能是 root），需要创建新根
                    page_id_t new_root_id;
                    Page *new_root_page = bpm_->NewPage(&new_root_id);
                    InternalNodeT root_view;
                    root_view.Init(new_root_page->GetData(), internal_max_size_);
                    root_view.Populate_New_Root(new_root_page->GetData(), internal_max_size_, up_key,
                                                current->GetPageId(), new_child_id);
                    // 更新根
                    root_page_id_ = new_root_id;
                    bpm_->UnpinPage(new_root_id, true);
                    std::cout << "[INS] create new root=" << new_root_id << std::endl;
                    break;
                }

                Page *parent_page = path[idx_in_path - 1];
                InternalNodeT parent_view;
                // 在父中插入 (up_key, new_child_id)
                parent_view.Insert(parent_page->GetData(), internal_max_size_, up_key, new_child_id, comparator_);
                std::cout << "[INS] insert up_key into parent page=" << parent_page->GetPageId() << std::endl;

                if (!parent_view.IS_Full(parent_page->GetData(), internal_max_size_)) {
                    // 父未满，结束
                    break;
                }

                // 父满，继续向上分裂
                page_id_t new_internal_id;
                Page *new_internal_page = bpm_->NewPage(&new_internal_id);
                InternalNodeT new_internal_view;
                new_internal_view.Init(new_internal_page->GetData(), internal_max_size_);
                auto up_key_next = parent_view.Split(parent_page->GetData(), new_internal_page->GetData(), internal_max_size_);
                // 释放新页
                bpm_->UnpinPage(new_internal_id, true);
                std::cout << "[INS] split internal parent page=" << parent_page->GetPageId() << " new_internal=" << new_internal_id << std::endl;

                // 为下一轮准备
                current = parent_page;
                up_key = up_key_next;
                new_child_id = new_internal_id;
                // 循环继续，尝试将 (up_key, new_child_id) 插入更高父
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
            std::cout << "[GET] key=" << key << " leaf=" << leaf->GetPageId() << " found=" << (found?1:0) << std::endl;
            if (found) {
                // dump index and arrays for verification
                int idx_dbg = leaf_view.Find_Key_Index(leaf->GetData(), leaf_max_size_, key, comparator_);
                int sz_dbg = leaf_view.Get_Size(leaf->GetData());
                auto keys_ptr = leaf_view.Keys_Ptr(leaf->GetData());
                auto vals_ptr = leaf_view.Values_Ptr(leaf->GetData(), leaf_max_size_);
                std::cout << "[GET][DBG] idx=" << idx_dbg << " size=" << sz_dbg << " value=" << *value << " keys=";
                for (int i = 0; i < sz_dbg; i++) std::cout << (i==0?"":" ") << keys_ptr[i];
                std::cout << " values=";
                for (int i = 0; i < sz_dbg; i++) std::cout << (i==0?"":" ") << vals_ptr[i];
                std::cout << std::endl;
            }
            if (!found) {
                int sz_dbg = leaf_view.Get_Size(leaf->GetData());
                std::cout << "[GET][DBG] leaf=" << leaf->GetPageId() << " size=" << sz_dbg << " keys=";
                auto keys_ptr = leaf_view.Keys_Ptr(leaf->GetData());
                for (int i = 0; i < sz_dbg; i++) {
                    std::cout << (i==0?"":" ") << keys_ptr[i];
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
            // 从最小键开始
            // 利用 Get_Leaf_Page 的 read 路径保证返回叶子已加 R 锁
            // 选择一个极小键：这里通过 InternalNode::Lookup 逻辑，直接从 root 向最左孩子走
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
        void Remove(const KeyType &key, Transaction *transaction = nullptr) {
            root_latch_.lock_shared();
            if (Is_Empty()) {
                root_latch_.unlock_shared();
                return;
            }
            if (transaction == nullptr) {
                throw std::logic_error("Remove requires a non-null transaction");
            }
            // 清理上一次操作可能遗留的页面集合/删除集合
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
            for (auto pid : delset) {
                bpm_->DeletePage(pid);
                std::cout << "[DEL] delete page id=" << pid << std::endl;
            }
            delset.clear();
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
            Iterator it = this->Begin(start_key);
            for (; it != this->End(); ++it) {
                if (comparator_((*it).first, end_key)) { // if current_key < end_key
                    result.push_back({(*it).first, (*it).second});
                } else {
                    break;
                }
            }
            return result;
        }

        /**
         * @brief 返回一个指向第一个不小于给定键的元素的迭代器
         */
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
        // 判断页面在给定操作下是否安全（不会触发分裂/合并）
        auto Is_Safe_Page(const char *page_data, bool is_leaf, bool is_root, Operation op) -> bool {
            if (op == Operation::Read) return true;
            NodeT node_view;
            int size = node_view.Get_Size(page_data);
            if (op == Operation::Insert) {
                if (is_leaf) return size < leaf_max_size_ - 1;
                return size < internal_max_size_;
            }
            // Remove 情况
            if (is_root) {
                if (is_leaf) return size > 1;
                return size > 2;
            }
            if (is_leaf) return size > (leaf_max_size_ / 2);
            return size > ((internal_max_size_ + 1) / 2);
        }

        // 释放事务中持有的写锁（从上到下顺序）；nullptr 表示 root 写锁
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

        // 从事务记录中获取指定 page_id 的 Page*（无需再次 Fetch）
        auto Get_Page_From_Transaction(page_id_t page_id, Transaction *txn) -> Page * {
            auto &ps = txn->GetPageSet();
            for (auto it = ps.rbegin(); it != ps.rend(); ++it) {
                Page *p = *it;
                if (p != nullptr && p->GetPageId() == page_id) return p;
            }
            throw std::logic_error("Parent page not found in transaction.");
        }

        // 释放兄弟页的短期写锁并 Unpin
        void Release_Siblings(Page *left, Page *right) {
            if (left) { std::cout << "[UF] release left sibling page=" << left->GetPageId() << std::endl; left->WUnlatch(); bpm_->UnpinPage(left->GetPageId(), true); }
            if (right){ std::cout << "[UF] release right sibling page=" << right->GetPageId() << std::endl; right->WUnlatch(); bpm_->UnpinPage(right->GetPageId(), true); }
        }

        // 下溢处理（借/并 + 递归）
        void Handle_Underflow(Page *page, Transaction *txn) {
            NodeT node_view;
            char *page_data = page->GetData();
            bool is_leaf = node_view.Is_Leaf(page_data);
            std::cout << "[UF] handle underflow at page=" << page->GetPageId() << " leaf=" << (is_leaf?1:0) << std::endl;

            // Root cases
            // In BPlusTree::Handle_Underflow
            if (page->GetPageId() == root_page_id_) {
                NodeT node_view;
                char* page_data = page->GetData();
                bool is_leaf = node_view.Is_Leaf(page_data);
                int size = node_view.Get_Size(page_data);

                // Case 1: Root is a leaf node.
                if (is_leaf) {
                    if (size == 0) { // If the last element is removed
                        root_page_id_ = INVALID_PAGE_ID;
                        txn->AddToDeletedPageSet(page->GetPageId());
                        std::cout << "[UF] root leaf becomes empty, set tree empty and mark delete page=" << page->GetPageId() << std::endl;
                    }
                    return; // Leaf root never underflows unless it's empty.
                }

                // Case 2: Root is an internal node.
                // It only underflows and causes tree height to shrink when it has 0 keys (1 child).
                if (!is_leaf && size == 0) {
                    InternalNodeT internal_view;
                    page_id_t new_root_id = internal_view.Child_At(page_data, internal_max_size_, 0);
                    root_page_id_ = new_root_id;
                    txn->AddToDeletedPageSet(page->GetPageId());
                    std::cout << "[UF] root internal has single child, new root=" << new_root_id << " delete old root=" << page->GetPageId() << std::endl;
                }
                return; // Root node processing ends here.
            }

            // Non-root: need parent and siblings
            // 从 transaction 的路径中找到当前页的父页：
            // 我们在第二轮是自顶向下按顺序 AddToPageSet 的，因此父页应当位于当前页之前的位置。
            Page *parent_page = nullptr;
            {
                auto &ps = txn->GetPageSet();
                // 复制到临时序列，便于用索引查找
                std::vector<Page *> path;
                path.reserve(ps.size());
                for (Page *p : ps) { path.push_back(p); }
                // 找到当前页在 path 中的下标
                int cur_idx = -1;
                for (int i = static_cast<int>(path.size()) - 1; i >= 0; --i) {
                    if (path[i] == page) { cur_idx = i; break; }
                }
                if (cur_idx == -1) {
                    throw std::logic_error("Current page not found in transaction path for underflow handling.");
                }
                // 向前寻找第一个非 nullptr 的条目即为父页
                for (int i = cur_idx - 1; i >= 0; --i) {
                    if (path[i] != nullptr) { parent_page = path[i]; break; }
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
            std::cout << "[UF] parent=" << parent_page->GetPageId() << " index_in_parent=" << index_in_parent << std::endl;

            // Fetch siblings (short-term locks)
            Page *left_page = nullptr;
            Page *right_page = nullptr;
            char *left_data = nullptr;
            char *right_data = nullptr;
            if (index_in_parent > 0) {
                page_id_t left_id = parent_view.Child_At(parent_data, internal_max_size_, index_in_parent - 1);
                left_page = bpm_->FetchPage(left_id);
                if (left_page) { left_page->WLatch(); left_data = left_page->GetData(); }
            }
            if (index_in_parent < parent_view.Get_Size(parent_data)) {
                page_id_t right_id = parent_view.Child_At(parent_data, internal_max_size_, index_in_parent + 1);
                right_page = bpm_->FetchPage(right_id);
                if (right_page) { right_page->WLatch(); right_data = right_page->GetData(); }
            }
            std::cout << "[UF] siblings L=" << (left_page?left_page->GetPageId():-1) << " R=" << (right_page?right_page->GetPageId():-1) << std::endl;

            // Try borrow from siblings
            bool borrowed = false;
            if (is_leaf) {
            LeafNodeT leaf_view;
                if (left_page) {
                    NodeT sibling_view;
                    if (sibling_view.Get_Size(left_data) > sibling_view.Get_Min_Size(leaf_max_size_)) {
                        leaf_view.Move_Last_From(page_data, left_data, leaf_max_size_);
                        // update parent separator key
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
                        // update parent separator key (first key of right sibling after move)
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
                        curr_view.Move_Last_From(page_data, left_data, internal_max_size_, parent_data, index_in_parent - 1);
                        borrowed = true;
                        std::cout << "[UF] borrowed from left sibling for internal" << std::endl;
                    }
                }
                if (!borrowed && right_page) {
                    NodeT sibling_view;
                    if (sibling_view.Get_Size(right_data) > sibling_view.Get_Min_Size(internal_max_size_)) {
                        curr_view.Move_First_From(page_data, right_data, internal_max_size_, parent_data, index_in_parent);
                        borrowed = true;
                        std::cout << "[UF] borrowed from right sibling for internal" << std::endl;
                    }
                }
            }

            if (borrowed) {
                Release_Siblings(left_page, right_page);
                return;
            }

            // Merge if cannot borrow
            if (is_leaf) {
                LeafNodeT leaf_view;
                if (left_page) {
                    // merge current into left
                    leaf_view.Merge(left_data, page_data, leaf_max_size_);
                    parent_view.Remove_At(parent_data, internal_max_size_, index_in_parent - 1);
                    txn->AddToDeletedPageSet(page->GetPageId());
                    std::cout << "[UF] merged current leaf into left" << std::endl;
                } else if (right_page) {
                    // merge right into current
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

            // If parent underflows, recursively handle
            NodeT parent_node_view;
            if (parent_node_view.Is_Underflow(parent_data, internal_max_size_)) {
                std::cout << "[UF] parent underflow, recurse parent page=" << parent_page->GetPageId() << std::endl;
                Handle_Underflow(parent_page, txn);
            }
        }

        // 两轮（乐观→悲观）获取叶子页，并按 latch crabbing 管理锁
        auto Get_Leaf_Page(const KeyType &key, Transaction *txn, Operation op, bool first_round = true) -> Page * {
            if (!first_round) {
                root_latch_.lock();
                if (txn == nullptr) {
                    throw std::logic_error("Write operations require a non-null transaction in second round.");
                }
                txn->AddToPageSet(nullptr);
            }

            page_id_t child = root_page_id_;
            Page *prev = nullptr;  // parent page in traversal

            while (true) {
                if (child == INVALID_PAGE_ID) {
                    throw std::runtime_error("Get_Leaf_Page: child is INVALID_PAGE_ID");
                }
                std::cout << "[GLP] fetch child=" << child << " first_round=" << (first_round?1:0) << " op=" << (int)op << std::endl;
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
                        // For Insert/Remove: latch leaf with write lock in first round
                        page->WLatch();
                        if (txn == nullptr) {
                            throw std::logic_error("Insert/Remove requires a non-null transaction.");
                        }
                        txn->AddToPageSet(page);
                        std::cout << "[GLP] WLatch leaf page=" << page->GetPageId() << std::endl;
                    } else {
                        // Read or internal page in first round
                        page->RLatch();
                        std::cout << "[GLP] RLatch page=" << page->GetPageId() << std::endl;
                    }
                    // release parent read lock as soon as child is latched
                    if (prev == nullptr) {
                        root_latch_.unlock_shared();
                    } else {
                        std::cout << "[GLP] release prev RLatch+Unpin page=" << prev->GetPageId() << std::endl;
                        prev->RUnlatch();
                        bpm_->UnpinPage(prev->GetPageId(), false);
                    }
                } else {
                    // second round: write-crabbing down the path
                    page->WLatch();
                    // In second round, always keep the full path for both Insert and Remove
                    txn->AddToPageSet(page);
                    std::cout << "[GLP] WLatch page=" << page->GetPageId() << std::endl;
                }

                if (is_leaf) {
                    if (first_round && !Is_Safe_Page(data, true, child == root_page_id_, op)) {
                        // not safe: release the only latched leaf (and root if any) and retry second round
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

            // 如果新分配的页面ID是0，我们需要重新分配一个非0的页面ID
            if (new_page_id == 0) {
                // 释放page_id=0的页面
                bpm_->DeletePage(0);
                // 重新分配一个新页面
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

            // 确保新创建的根页面立即刷新到磁盘
            bpm_->FlushPage(new_page_id);

            std::cout << "[DEBUG] Start_New_Tree: 页面已刷新到磁盘" << std::endl;

            // 立即更新元数据页面
            Page *meta_page = bpm_->FetchPage(0);
            *reinterpret_cast<page_id_t *>(meta_page->GetData()) = root_page_id_;
            bpm_->UnpinPage(0, true);
            bpm_->FlushPage(0);
            std::cout << "[DEBUG] Start_New_Tree: 元数据页面已更新，root_page_id_=" << root_page_id_ << std::endl;
        };

        /**
         * @brief 查找指定节点的父节点ID
         */
        auto Find_Parent_Page_Id(page_id_t child_id) const -> page_id_t {
            if (child_id == root_page_id_) {
                return INVALID_PAGE_ID; // 根节点没有父节点
            }

            // 使用递归搜索
            return Find_Parent_Recursive(root_page_id_, child_id);
        }

        /**
         * @brief 递归查找父节点
         */
        auto Find_Parent_Recursive(page_id_t current_id, page_id_t target_child_id) const -> page_id_t {
            // 添加边界检查，防止无限递归
            if (current_id == INVALID_PAGE_ID) {
                return INVALID_PAGE_ID;
            }

            PageGuard guard = bpm_->FetchPageGuard(current_id);
            if (!guard) return INVALID_PAGE_ID;

            const char *data = guard.GetData();
            NodeT node_view;

            if (node_view.Is_Leaf(data)) {
                return INVALID_PAGE_ID; // 叶子节点不可能是父节点
            }

            InternalNodeT internal_view;
            int size = internal_view.Get_Size(data);

            for (int i = 0; i <= size; ++i) {  // 注意：这里应该是 <= size，因为子节点数量比键数量多1
                page_id_t child_page_id = internal_view.Child_At(data, internal_max_size_, i);
                if (child_page_id == target_child_id) {
                    return current_id; // 找到了父节点
                }
            }

            static int recursion_depth = 0;
            if (recursion_depth > 100) {  // 防止无限递归
                return INVALID_PAGE_ID;
            }

            recursion_depth++;
            for (int i = 0; i <= size; ++i) {  // 注意：这里应该是 <= size
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

        auto Find_Leaf_Guard(const KeyType &key) const -> PageGuard {
            page_id_t current_page_id = root_page_id_;
            int depth = 0; // 防止无限循环
            const int MAX_DEPTH = 100;

            while (depth < MAX_DEPTH) {
                PageGuard guard = bpm_->FetchPageGuard(current_page_id);
                if (!guard) {
                    // 如果无法获取页面，返回空的PageGuard
                    return PageGuard(bpm_.get(), nullptr);
                }

                const char *data = guard.GetData();
                NodeT node_view;
                if (node_view.Is_Leaf(data)) return guard;

                InternalNodeT internal_view;
                current_page_id = internal_view.Lookup(data, internal_max_size_, key, comparator_);

                if (current_page_id == INVALID_PAGE_ID) {
                    // 如果Lookup返回无效页面ID，说明树结构有问题
                    return PageGuard(bpm_.get(), nullptr);
                }

                depth++;
            }

            return PageGuard(bpm_.get(), nullptr);
        }

        /**
         * @brief 处理单个节点的分裂（完整版本）
         */
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

            // 检查是否需要创建新的根节点
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
                // 对于非根节点的分裂，需要向上传播分裂
                // 首先找到父节点
                page_id_t parent_id = Find_Parent_Page_Id(node_guard.GetPageId());
                if (parent_id != INVALID_PAGE_ID) {
                    PageGuard parent_guard = bpm_->FetchPageGuard(parent_id);
                    if (parent_guard) {
                        InternalNodeT parent_view;
                        parent_view.Insert(parent_guard.GetData(), internal_max_size_, key_to_parent, new_sibling_id,
                                           comparator_);
                        parent_guard.SetDirty();

                        // 检查父节点是否需要分裂
                        if (parent_view.IS_Full(parent_guard.GetData(), internal_max_size_)) {
                            Handle_Split(std::move(parent_guard));
                        }
                    } else {
                        // 如果无法获取父节点，这可能表明树结构有问题
                        // 在这种情况下，我们暂时不处理分裂传播
                        // 这可能会导致树结构不一致，但至少不会崩溃
                    }
                } else {
                    // 如果找不到父节点，这可能表明树结构有问题
                    // 在这种情况下，我们暂时不处理分裂传播
                    // 这可能会导致树结构不一致，但至少不会崩溃
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

        /**
         * @brief 处理单个节点的下溢（重分配或合并）
         */
        void Handle_Underflow(PageGuard &&node_guard) {
            page_id_t node_id = node_guard.GetPageId();

            // --- 1. 处理根节点下溢 ---
            if (node_id == root_page_id_) {
                NodeT node_view;
                const char *node_data = node_guard.GetData();

                // 情况1: 根是内部节点且变空 -> 降低树高
                if (!node_view.Is_Leaf(node_data) && node_view.Get_Size(node_data) == 0) {
                    InternalNodeT internal_view;
                    root_page_id_ = internal_view.Move_First_Child(node_guard.GetData(), internal_max_size_);
                    // 此时旧的根页面可以删除了
                    bpm_->DeletePage(node_id);
                }
                    // 情况2: 根是叶子节点且变空 -> 树变空
                else if (node_view.Is_Leaf(node_data) && node_view.Get_Size(node_data) == 0) {
                    root_page_id_ = INVALID_PAGE_ID;
                    bpm_->DeletePage(node_id);
                }
                return;
            }

            // --- 2. 获取父节点信息 ---
            // 这里简化处理，实际需要从根节点开始查找父节点
            // 为了简化，我们暂时不处理非根节点的下溢
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

        // --- 并发控制 ---
        // 根节点锁，保护 root_page_id_ 的访问
        mutable std::shared_mutex root_latch_;
    };
} // namespace bptree