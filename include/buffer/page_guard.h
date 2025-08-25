#pragma once

#include "page.h"

namespace bptree {

    class BufferPoolManager;

    class PageGuard {
    public:
        enum class LatchMode { None, Read, Write };

        PageGuard(BufferPoolManager *bpm, Page *page);

        PageGuard(BufferPoolManager *bpm, Page *page, LatchMode latch_mode);

        ~PageGuard();

        PageGuard(PageGuard &&other) noexcept;//移动构造

        PageGuard &operator=(PageGuard &&other) noexcept;//移动赋值

        auto GetData() -> char * { return page_ != nullptr ? page_->GetData() : nullptr; }

        auto GetData() const -> const char * { return page_ != nullptr ? page_->GetData() : nullptr; }

        auto GetPageId() const -> page_id_t { return page_ != nullptr ? page_->GetPageId() : INVALID_PAGE_ID; }

        void SetDirty() { is_dirty_ = true; }

        explicit operator bool() const { return page_ != nullptr; }

        // Release latch (if any) and unpin immediately, making this guard empty
        void Drop();

        PageGuard(const PageGuard &) = delete;

        PageGuard &operator=(const PageGuard &) = delete;

    private:
        BufferPoolManager *bpm_;
        Page *page_;
        bool is_dirty_ = false;
        LatchMode latch_mode_ = LatchMode::None;
    };

}