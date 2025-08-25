//
// Created by lenovo on 2025/8/5.
//

#pragma once

#include <list>
#include <mutex>
#include <unordered_map>
#include "page.h"
#include "replacer.h"
#include "disk_manager.h"
#include "config.h"
#include "page_guard.h"

namespace bptree {

/**
 * @class BufferPoolManager
 * @brief 负责管理缓冲池中的页面，协调内存与磁盘之间的数据交换。
 *
 * BufferPoolManager 将磁盘上的页面缓存到内存中，以加速访问。
 * 它使用页面替换策略（如LRU）来决定在缓冲池满时应驱逐哪个页面。
 * 这个类是线程安全的。
 */
    class BufferPoolManager {
    public:
        /**
         * @brief 构造函数。
         * @param pool_size 缓冲池的大小（帧的数量）。
         * @param disk_manager 指向磁盘管理器的指针。
         * @param replacer 页面替换策略（例如 LRUReplacer）。
         */
        BufferPoolManager(size_t pool_size, DiskManager* disk_manager, Replacer* replacer);

        /**
         * @brief 析构函数。
         *
         * 会将所有脏页刷回磁盘。
         */
        ~BufferPoolManager();

        // 禁用拷贝和赋值操作。
        BufferPoolManager(const BufferPoolManager&) = delete;
        BufferPoolManager& operator=(const BufferPoolManager&) = delete;

        /**
         * @brief 从缓冲池中获取一个页面。
         *
         * 如果页面已在缓冲池中，则增加其 pin_count 并返回。
         * 如果页面不在缓冲池中，则从磁盘读取它，可能需要驱逐一个旧页面。
         *
         * @param page_id 要获取的页面ID。
         * @return 指向 Page 对象的指针；如果无法获取（例如缓冲池已满且所有页面都被pin住），则返回 nullptr。
         */
        auto FetchPage(page_id_t page_id) -> Page*;

        /**
         * @brief 解除一个页面的固定 (Unpin)。
         *
         * @param page_id 要 unpin 的页面ID。
         * @param is_dirty 如果页面内容被修改过，则为 true。
         * @return 如果操作成功，则返回 true；如果 page_id 无效或 pin_count 已为0，则返回 false。
         */
        auto UnpinPage(page_id_t page_id, bool is_dirty) -> bool;

        /**
         * @brief 将一个脏页强制刷回磁盘。
         *
         * @param page_id 要刷回的页面ID。
         * @return 如果操作成功，则返回 true。
         */
        auto FlushPage(page_id_t page_id) -> bool;

        /**
         * @brief 在缓冲池中创建一个新页面。
         *
         * @param[out] page_id 指向用于存储新页面ID的指针。
         * @return 指向新 Page 对象的指针；如果无法创建（缓冲池已满且无法驱逐），则返回 nullptr。
         */
        auto NewPage(page_id_t* page_id) -> Page*;

        /**
         * @brief 从缓冲池和磁盘中删除一个页面。
         *
         * @param page_id 要删除的页面ID。
         * @return 如果操作成功，则返回 true。
         */
        auto DeletePage(page_id_t page_id) -> bool;

        /**
         * @brief 将缓冲池中所有的脏页刷回磁盘。
         */
        void FlushAllPages();

        /**
         * @brief 获取一个页面的 PageGuard。
         * 这是一个便利的包装函数，直接返回一个管理页面的 guard。
         */
        auto FetchPageGuard(page_id_t page_id) -> PageGuard {
            return PageGuard(this, FetchPage(page_id));
        }

        /**
         * @brief 获取一个页面的只读 PageGuard（获取共享锁）。
         */
        auto FetchPageReadGuard(page_id_t page_id) -> PageGuard {
            Page* page = FetchPage(page_id);
            if (page == nullptr) { return PageGuard(this, nullptr); }
            page->AcquireReadLatch();
            return PageGuard(this, page, PageGuard::LatchMode::Read);
        }

        /**
         * @brief 获取一个页面的可写 PageGuard（获取独占锁）。
         */
        auto FetchPageWriteGuard(page_id_t page_id) -> PageGuard {
            Page* page = FetchPage(page_id);
            if (page == nullptr) { return PageGuard(this, nullptr); }
            page->AcquireWriteLatch();
            return PageGuard(this, page, PageGuard::LatchMode::Write);
        }

        /**
         * @brief 创建一个新页面的 PageGuard。
         */
        auto NewPageGuard(page_id_t* page_id) -> PageGuard {
            return PageGuard(this, NewPage(page_id));
        }

        /**
         * @brief 创建一个新页面并返回写锁保护。
         */
        auto NewPageWriteGuard(page_id_t* page_id) -> PageGuard {
            Page* page = NewPage(page_id);
            if (page == nullptr) { return PageGuard(this, nullptr); }
            page->AcquireWriteLatch();
            return PageGuard(this, page, PageGuard::LatchMode::Write);
        }

    private:
        /**
         * @brief 私有辅助函数：从 replacer 或空闲列表中找到一个可用的帧。
         * @return 可用帧的ID；如果找不到，则返回 std::nullopt。
         */
        auto FindVictimFrame() -> std::optional<frame_id_t>;

        // --- 成员变量 ---

        // 缓冲池的大小（帧的数量）
        const size_t pool_size_;

        // 缓冲池的帧数组，实际存储 Page 对象的地方
        Page* pages_;

        // 指向磁盘管理器的指针
        DiskManager* disk_manager_;

        // 指向页面替换策略的指针
        Replacer* replacer_;

        // 页面表 (Page Table)，用于将 page_id 映射到 frame_id
        std::unordered_map<page_id_t, frame_id_t> page_table_;

        // 空闲帧列表。当有帧被释放时，会添加到这里，优先被 NewPage 使用。
        std::list<frame_id_t> free_list_;

        // 用于保护缓冲池内部数据结构的互斥锁，确保线程安全
        std::mutex latch_;
    };

} // namespace bptree