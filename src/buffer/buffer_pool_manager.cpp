//
// Created by lenovo on 2025/8/5.
//
// src/buffer/buffer_pool_manager.cpp

#include "buffer_pool_manager.h"
#include <stdexcept>

namespace bptree {

    BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager* disk_manager, Replacer* replacer)
            : pool_size_(pool_size), disk_manager_(disk_manager), replacer_(replacer) {
        // 在堆上分配缓冲池的帧数组
        pages_ = new Page[pool_size_];

        // 初始化时，所有帧都是空闲的
        for (size_t i = 0; i < pool_size_; ++i) {
            free_list_.push_back(static_cast<frame_id_t>(i));
        }
    }

    BufferPoolManager::~BufferPoolManager() {
        // 在析构前，确保所有脏页都被写回磁盘
        FlushAllPages();
        // 释放帧数组的内存
        delete[] pages_;
    }

    auto BufferPoolManager::FetchPage(page_id_t page_id) -> Page* {
        // 1. 加锁，保护内部数据结构
        std::lock_guard<std::mutex> guard(latch_);

        // 2. 检查页面是否已经在缓冲池中 (在 page_table 中查找)
        auto it = page_table_.find(page_id);
        if (it != page_table_.end()) {
            // 页面命中缓存
            frame_id_t frame_id = it->second;
            Page* page = &pages_[frame_id];
            // 增加 pin 计数，并通知 replacer
            page->IncPinCount();
            replacer_->Pin(frame_id);
            return page;
        }

        // 3. 页面不在缓冲池中，需要从磁盘加载
        // 首先，找到一个可用的帧
        frame_id_t frame_id_to_use;
        if (!free_list_.empty()) {
            // 优先使用空闲帧
            frame_id_to_use = free_list_.front();
            free_list_.pop_front();
        } else {
            // 如果没有空闲帧，则尝试从 replacer 中驱逐一个
            if (!replacer_->Victim(&frame_id_to_use)) {
                // 如果 replacer 也为空 (意味着所有页面都被 pin 住)，则无法获取页面
                return nullptr;
            }

            // 驱逐旧页面
            Page* old_page = &pages_[frame_id_to_use];
            if (old_page->IsDirty()) {
                // 如果旧页面是脏的，写回磁盘
                disk_manager_->WritePage(old_page->GetPageId(), old_page->GetData());
            }
            // 从 page_table 中移除旧页面的映射
            page_table_.erase(old_page->GetPageId());
        }

        // 4. 在找到的帧中加载新页面
        Page* new_page = &pages_[frame_id_to_use];
        // 从磁盘读取页面内容
        disk_manager_->ReadPage(page_id, new_page->GetData());

        // 5. 更新新页面的元数据
        new_page->SetPageId(page_id);
        new_page->SetDirty(false);
        new_page->SetPinCount(1); // Fetch后pin_count为1

        // 6. 更新 page_table 和 replacer
        page_table_[page_id] = frame_id_to_use;
        replacer_->Pin(frame_id_to_use); // 新加载的页面立即被 pin 住

        return new_page;
    }

    auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) -> bool {
        std::lock_guard<std::mutex> guard(latch_);

        // 1. 查找页面
        auto it = page_table_.find(page_id);
        if (it == page_table_.end()) {
            return false; // 页面不在缓冲池中
        }

        frame_id_t frame_id = it->second;
        Page* page = &pages_[frame_id];

        // 2. 检查 pin_count
        if (page->GetPinCount() <= 0) {
            return false; // 一个未被 pin 的页面不能被 unpin
        }

        // 3. 减少 pin_count
        page->DecPinCount();

        // 4. 如果 is_dirty 为 true，则设置页面的脏位
        if (is_dirty) {
            page->SetDirty(true);
        }

        // 5. 如果 pin_count 降为 0，则通知 replacer 该帧可以被替换
        if (page->GetPinCount() == 0) {
            replacer_->Unpin(frame_id);
        }

        return true;
    }

    auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
        std::lock_guard<std::mutex> guard(latch_);

        auto it = page_table_.find(page_id);
        if (it == page_table_.end()) {
            return false; // 页面不在缓冲池中
        }

        frame_id_t frame_id = it->second;
        Page* page = &pages_[frame_id];

        // 将页面内容写回磁盘
        disk_manager_->WritePage(page->GetPageId(), page->GetData());

        // 写回后，页面的脏位可以被清除
        page->SetDirty(false);

        return true;
    }

    auto BufferPoolManager::NewPage(page_id_t* page_id) -> Page* {
        std::lock_guard<std::mutex> guard(latch_);

        // 1. 找到一个可用的帧
        frame_id_t frame_id_to_use;
        if (!free_list_.empty()) {
            frame_id_to_use = free_list_.front();
            free_list_.pop_front();
        } else {
            if (!replacer_->Victim(&frame_id_to_use)) {
                return nullptr; // 所有页面都被 pin 住
            }

            Page* old_page = &pages_[frame_id_to_use];
            if (old_page->IsDirty()) {
                disk_manager_->WritePage(old_page->GetPageId(), old_page->GetData());
            }
            page_table_.erase(old_page->GetPageId());
        }

        // 2. 从 disk_manager 分配一个新的 page_id
        *page_id = disk_manager_->AllocatePage();

        // 3. 在找到的帧中初始化新页面
        Page* new_page = &pages_[frame_id_to_use];
        new_page->ResetMemory(); // 清空数据和元数据
        new_page->SetPageId(*page_id);
        new_page->SetPinCount(1);
        new_page->SetDirty(false);

        // 4. 更新 page_table 和 replacer
        page_table_[*page_id] = frame_id_to_use;
        replacer_->Pin(frame_id_to_use);

        return new_page;
    }

    auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
        std::lock_guard<std::mutex> guard(latch_);

        // 1. 查找页面
        auto it = page_table_.find(page_id);
        if (it == page_table_.end()) {
            return true; // 页面不在缓冲池中，我们认为它已经被删除了
        }

        frame_id_t frame_id = it->second;
        Page* page = &pages_[frame_id];

        // 2. 检查页面是否被 pin 住
        if (page->GetPinCount() > 0) {
            return false; // 不能删除一个正在被使用的页面
        }

        // 3. 从 page_table 和 replacer 中移除
        page_table_.erase(it);
        replacer_->Pin(frame_id); // 从可替换列表中移除

        // 4. 重置页面内存并将其帧加入空闲列表
        page->ResetMemory();
        free_list_.push_back(frame_id);

        // 5. 通知 disk_manager 释放磁盘空间
        disk_manager_->DeallocatePage(page_id);

        return true;
    }

    void BufferPoolManager::FlushAllPages() {
        std::lock_guard<std::mutex> guard(latch_);

        // 遍历 page_table 中的所有页面
        for (const auto& pair : page_table_) {
            page_id_t page_id = pair.first;
            frame_id_t frame_id = pair.second;
            Page* page = &pages_[frame_id];

            if (page->IsDirty()) {
                disk_manager_->WritePage(page_id, page->GetData());
                page->SetDirty(false);
            }
        }
    }

} // namespace bptree