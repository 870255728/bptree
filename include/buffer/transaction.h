#pragma once

#include <deque>
#include <unordered_set>
#include "page.h"
#include "b_plus_tree.h" // Forward declare to avoid circular deps if needed

namespace bptree {
    class Transaction {
    public:
        Transaction() = default;

        // Returns the set of pages latched by this transaction.
        auto GetPageSet() -> std::deque<Page *> & { return page_set_; }

        // Adds a page to the latch set.
        void AddToPageSet(Page *page) { page_set_.push_back(page); }

        // Returns the set of pages to be deleted after operation completes.
        auto GetDeletedPageSet() -> std::unordered_set<page_id_t> & { return deleted_page_set_; }

        // Adds a page id into the deleted set.
        void AddToDeletedPageSet(page_id_t page_id) { deleted_page_set_.insert(page_id); }

    private:
        // Pages latched by the transaction. A deque is used for efficient
        // additions to the back.
        std::deque<Page *> page_set_;

        // Pages that should be deleted after the write operation completes.
        std::unordered_set<page_id_t> deleted_page_set_;
    };
}