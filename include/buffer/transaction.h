#pragma once

#include <deque>
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

    private:
        // Pages latched by the transaction. A deque is used for efficient
        // additions to the back.
        std::deque<Page *> page_set_;
    };
}