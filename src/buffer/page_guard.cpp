#include "page_guard.h"
#include "buffer_pool_manager.h"

namespace bptree {

    PageGuard::PageGuard(BufferPoolManager *bpm, Page *page)
            : bpm_(bpm), page_(page) {}

    PageGuard::~PageGuard() {
        if (page_ != nullptr) {
            bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
        }
    }

    PageGuard::PageGuard(PageGuard &&other) noexcept {
        bpm_ = other.bpm_;
        page_ = other.page_;
        is_dirty_ = other.is_dirty_;
        other.page_ = nullptr;
    }

    PageGuard &PageGuard::operator=(PageGuard &&other) noexcept {
        if (this != &other) {
            if (page_ != nullptr) {
                bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
            }
            bpm_ = other.bpm_;
            page_ = other.page_;
            is_dirty_ = other.is_dirty_;
            other.page_ = nullptr;
        }
        return *this;
    }
}