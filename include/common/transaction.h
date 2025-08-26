#pragma once

#include <vector>
#include <mutex>
#include "page.h"

namespace bptree {

/**
 * @class Transaction
 * @brief 表示一个数据库事务，管理当前线程持有的锁和页面
 */
class Transaction {
public:
    Transaction() = default;
    ~Transaction() = default;

    /**
     * @brief 将页面添加到当前事务的页面集合中
     * @param page 要添加的页面指针
     */
    void AddIntoPageSet(Page* page) {
        std::lock_guard<std::mutex> lock(page_set_mutex_);
        page_set_.push_back(page);
    }

    /**
     * @brief 将页面添加到待删除页面集合中
     * @param page_id 要删除的页面ID
     */
    void AddIntoDeletedPageSet(page_id_t page_id) {
        std::lock_guard<std::mutex> lock(deleted_page_set_mutex_);
        deleted_page_set_.push_back(page_id);
    }

    /**
     * @brief 获取当前事务持有的所有页面
     * @return 页面指针的向量
     */
    auto GetPageSet() const -> const std::vector<Page*>& {
        return page_set_;
    }

    /**
     * @brief 获取待删除的页面ID集合
     * @return 页面ID的向量
     */
    auto GetDeletedPageSet() const -> const std::vector<page_id_t>& {
        return deleted_page_set_;
    }

    /**
     * @brief 清空页面集合
     */
    void ClearPageSet() {
        std::lock_guard<std::mutex> lock(page_set_mutex_);
        page_set_.clear();
    }

    /**
     * @brief 清空待删除页面集合
     */
    void ClearDeletedPageSet() {
        std::lock_guard<std::mutex> lock(deleted_page_set_mutex_);
        deleted_page_set_.clear();
    }

private:
    // 当前事务持有的页面集合
    mutable std::mutex page_set_mutex_;
    std::vector<Page*> page_set_;

    // 待删除的页面ID集合
    mutable std::mutex deleted_page_set_mutex_;
    std::vector<page_id_t> deleted_page_set_;
};

} // namespace bptree
