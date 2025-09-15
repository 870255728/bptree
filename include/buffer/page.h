// include/buffer/page.h

#pragma once

#include <cstdint>
#include <cstring> // for memset
#include <shared_mutex> // for std::shared_mutex
#include "config.h" // 引入 PAGE_SIZE 和 page_id_t

namespace bptree {

    class Page {
    public:
        Page() {
            ResetMemory();
        }

        ~Page() = default;
        auto GetPageId() const -> page_id_t {
            return page_id_;
        }
        void SetPageId(page_id_t page_id) {
            page_id_ = page_id;
        }

        auto GetPinCount() const -> int {
            return pin_count_;
        }

        void IncPinCount() {
            pin_count_++;
        }

        void DecPinCount() {
            // 确保 pin_count 不会变成负数
            if (pin_count_ > 0) {
                pin_count_--;
            }
        }

        auto IsDirty() const -> bool {
            return is_dirty_;
        }

        void SetDirty(bool is_dirty) {
            is_dirty_ = is_dirty;
        }

        auto GetData() -> char* {
            return data_;
        }

        auto GetData() const -> const char* {
            return data_;
        }

        void ResetMemory() {
            memset(data_, 0, PAGE_SIZE);
            page_id_ = INVALID_PAGE_ID;
            pin_count_ = 0;
            is_dirty_ = false;
        }

        auto SetPinCount(int count)->int {
            pin_count_ = count;
            return pin_count_;
        }

        void RLatch() {
            latch_.lock_shared();
        }

        void RUnlatch() {
            latch_.unlock_shared();
        }

        void WLatch() {
            latch_.lock();
        }

        void WUnlatch() {
            latch_.unlock();
        }

        auto TryWLatch() -> bool {
            return latch_.try_lock();
        }

        auto TryRLatch() -> bool {
            return latch_.try_lock_shared();
        }

    private:
        mutable std::shared_mutex latch_;
        alignas(PAGE_SIZE) char data_[PAGE_SIZE];
        page_id_t page_id_ = INVALID_PAGE_ID;

        int pin_count_ = 0;

        bool is_dirty_ = false;
    };

} // namespace bptree