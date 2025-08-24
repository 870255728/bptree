#pragma once

#include "page.h"

namespace bptree {

    class BufferPoolManager;

    class PageGuard {
    public:

        PageGuard(BufferPoolManager *bpm, Page *page);

        ~PageGuard();

        PageGuard(PageGuard &&other) noexcept;//移动构造

        PageGuard &operator=(PageGuard &&other) noexcept;//移动赋值

        auto GetData() -> char * { return page_ != nullptr ? page_->GetData() : nullptr; }

        auto GetData() const -> const char * { return page_ != nullptr ? page_->GetData() : nullptr; }

        auto GetPageId() const -> page_id_t { return page_ != nullptr ? page_->GetPageId() : INVALID_PAGE_ID; }

        void SetDirty() { is_dirty_ = true; }

        explicit operator bool() const { return page_ != nullptr; }

        PageGuard(const PageGuard &) = delete;

        PageGuard &operator=(const PageGuard &) = delete;

    private:
        BufferPoolManager *bpm_;
        Page *page_;
        bool is_dirty_ = false;
    };

}