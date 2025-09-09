// include/buffer/page.h

#pragma once

#include <cstdint>
#include <cstring> // for memset
#include <shared_mutex> // for std::shared_mutex
#include "config.h" // 引入 PAGE_SIZE 和 page_id_t

namespace bptree {

/**
 * @class Page
 * @brief  表示缓冲池中的一个页面，包含数据区域和元数据。
 *
 * Page 类是缓冲池管理器的基本单元。它封装了实际存储在内存中的
 * 页面数据，以及与页面相关的元数据，如页面ID、pin计数器和脏位。
 */
    class Page {
    public:
        /**
         * @brief 构造函数，创建一个空的页面。
         */
        Page() {
            ResetMemory();
        }

        /**
         * @brief 析构函数。
         */
        ~Page() = default;

        // --- 访问元数据的方法 ---

        /**
         * @brief 获取页面的ID
         * @return 页面ID
         */
        auto GetPageId() const -> page_id_t {
            return page_id_;
        }

        /**
         * @brief 设置页面的ID
         * @param page_id 要设置的页面ID
         */
        void SetPageId(page_id_t page_id) {
            page_id_ = page_id;
        }

        /**
         * @brief 获取页面的pin计数器
         * @return pin计数器的值
         */
        auto GetPinCount() const -> int {
            return pin_count_;
        }

        /**
         * @brief 增加页面的 pin 计数
         */
        void IncPinCount() {
            pin_count_++;
        }

        /**
         * @brief 减少页面的 pin 计数
         */
        void DecPinCount() {
            // 确保 pin_count 不会变成负数
            if (pin_count_ > 0) {
                pin_count_--;
            }
        }

        /**
         * @brief 获取页面是否被标记为脏
         * @return 如果页面是脏的则返回 true，否则返回 false
         */
        auto IsDirty() const -> bool {
            return is_dirty_;
        }

        /**
         * @brief 设置页面的脏位
         * @param is_dirty 要设置的脏位状态
         */
        void SetDirty(bool is_dirty) {
            is_dirty_ = is_dirty;
        }

        /**
         * @brief 获取页面数据区的指针
         * @return 指向页面数据区的 char* 指针
         */
        auto GetData() -> char* {
            return data_;
        }

        /**
         * @brief 获取页面数据区的 const char* 指针 (const 版本)
         * @return 指向页面数据区的 const char* 指针
         */
        auto GetData() const -> const char* {
            return data_;
        }

        /**
         * @brief 将页面内容清空为0 (用于初始化或页面无效化)
         */
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

        // --- 并发控制方法 ---

        /**
         * @brief 获取读锁
         */
        void RLatch() {
            latch_.lock_shared();
        }

        /**
         * @brief 释放读锁
         */
        void RUnlatch() {
            latch_.unlock_shared();
        }

        /**
         * @brief 获取写锁
         */
        void WLatch() {
            latch_.lock();
        }

        /**
         * @brief 释放写锁
         */
        void WUnlatch() {
            latch_.unlock();
        }

        /**
         * @brief 尝试获取写锁
         * @return 如果成功获取锁返回true，否则返回false
         */
        auto TryWLatch() -> bool {
            return latch_.try_lock();
        }

        /**
         * @brief 尝试获取读锁
         * @return 如果成功获取锁返回true，否则返回false
         */
        auto TryRLatch() -> bool {
            return latch_.try_lock_shared();
        }

    private:
        // --- 成员变量 ---

        // 页面级别的读写锁，用于并发控制
        mutable std::shared_mutex latch_;

        // 实际存储页面数据的缓冲区
        // 使用 alignas 来确保数据区是内存对齐的，这对于某些操作可以提高性能。
        alignas(PAGE_SIZE) char data_[PAGE_SIZE];

        // 页面的ID。
        page_id_t page_id_ = INVALID_PAGE_ID;

        // 页面被引用的次数。当一个页面被 Fetch 后，
        // pin_count 会增加。当 Unpin 后， pin_count 减少。
        // 如果 pin_count 为 0，则可以被驱逐。
        int pin_count_ = 0;

        // 脏位。表示页面是否被修改过，需要写回磁盘。
        bool is_dirty_ = false;
    };

} // namespace bptree