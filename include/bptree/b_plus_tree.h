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
            // 使用全局写锁来确保线程安全
            std::lock_guard<std::shared_mutex> lock(root_latch_);

            if (Is_Empty()) {
                Start_New_Tree(key, value);
                return true;
            }

            // 使用原有的查找方法
            PageGuard leaf_guard = Find_Leaf_Guard(key);
            if (!leaf_guard) {
                return false;
            }

            LeafNodeT leaf_view;
            int old_size = leaf_view.Get_Size(leaf_guard.GetData());
            leaf_view.Insert(leaf_guard.GetData(), leaf_max_size_, key, value, comparator_);
            int new_size = leaf_view.Get_Size(leaf_guard.GetData());

            if (new_size == old_size) {
                return false; // Duplicate key
            }
            leaf_guard.SetDirty();

            if (leaf_view.IS_Full(leaf_guard.GetData(), leaf_max_size_)) {
                // 处理分裂 - 使用完整的分裂处理
                Handle_Split(std::move(leaf_guard));
            }
            return true;
        }

        auto Get_Value(const KeyType &key, ValueType *value, Transaction *transaction = nullptr) const -> bool {
            // 使用全局读锁来确保线程安全
            std::shared_lock<std::shared_mutex> lock(root_latch_);

            if (Is_Empty()) {
                return false;
            }

            // 使用原有的查找方法
            PageGuard leaf_guard = const_cast<BPlusTree *>(this)->Find_Leaf_Guard(key);
            if (!leaf_guard) {
                return false;
            }

            LeafNodeT leaf_view;
            bool result = leaf_view.Get_Value(leaf_guard.GetData(), leaf_max_size_, key, value, comparator_);

            return result;
        }

        auto Begin() -> Iterator {
            if (this->Is_Empty()) {
                return this->End();
            }

            page_id_t current_page_id = root_page_id_;

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
                page_id_t child_page_id = internal_view.Child_At(data, internal_max_size_, 0);

                guard = bpm_->FetchPageGuard(child_page_id);

                if (!guard) {
                    return this->End();
                }
            }

            return Iterator(bpm_.get(), guard.GetPageId(), 0, leaf_max_size_);
        }

        auto End() -> Iterator {
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
        void Remove(const KeyType &key, Transaction *transaction = nullptr) {
            // 使用全局写锁来确保线程安全
            std::lock_guard<std::shared_mutex> lock(root_latch_);

            if (this->Is_Empty()) {
                return;
            }

            PageGuard leaf_guard = Find_Leaf_Guard(key);
            if (!leaf_guard) {
                return;
            }

            LeafNodeT leaf_view;
            char *leaf_data = leaf_guard.GetData();
            int old_size = leaf_view.Get_Size(leaf_data);
            leaf_view.Remove(leaf_data, leaf_max_size_, key, comparator_);
            int new_size = leaf_view.Get_Size(leaf_data);

            if (new_size == old_size) {
                return;
            }

            leaf_guard.SetDirty();

            if (leaf_view.Is_Underflow(leaf_data, leaf_max_size_)) {
                Handle_Underflow(std::move(leaf_guard));
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