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

    class BufferPoolManager {
    public:
        BufferPoolManager(size_t pool_size, DiskManager* disk_manager, Replacer* replacer);

        ~BufferPoolManager();

        BufferPoolManager(const BufferPoolManager&) = delete;
        BufferPoolManager& operator=(const BufferPoolManager&) = delete;

        auto FetchPage(page_id_t page_id) -> Page*;

        auto UnpinPage(page_id_t page_id, bool is_dirty) -> bool;

        auto FlushPage(page_id_t page_id) -> bool;

        auto NewPage(page_id_t* page_id) -> Page*;

        auto DeletePage(page_id_t page_id) -> bool;

        void FlushAllPages();

        auto FetchPageGuard(page_id_t page_id) -> PageGuard {
            return PageGuard(this, FetchPage(page_id));
        }

        auto NewPageGuard(page_id_t* page_id) -> PageGuard {
            return PageGuard(this, NewPage(page_id));
        }

    private:
        auto FindVictimFrame() -> std::optional<frame_id_t>;

        const size_t pool_size_;

        Page* pages_;

        DiskManager* disk_manager_;

        Replacer* replacer_;

        std::unordered_map<page_id_t, frame_id_t> page_table_;

        std::list<frame_id_t> free_list_;

        std::mutex latch_;
    };

} // namespace bptree