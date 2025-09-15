#pragma once

#include <deque>
#include <unordered_set>
#include "page.h"
#include "b_plus_tree.h" // Forward declare to avoid circular deps if needed

namespace bptree {
    class Transaction {
    public:
        Transaction() = default;

        auto GetPageSet() -> std::deque<Page *> & { return page_set_; }

        void AddToPageSet(Page *page) { page_set_.push_back(page); }

        auto GetDeletedPageSet() -> std::unordered_set<page_id_t> & { return deleted_page_set_; }

        void AddToDeletedPageSet(page_id_t page_id) { deleted_page_set_.insert(page_id); }

    private:
        std::deque<Page *> page_set_;

        std::unordered_set<page_id_t> deleted_page_set_;
    };
}