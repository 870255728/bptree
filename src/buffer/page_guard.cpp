#include "page_guard.h"
#include "buffer_pool_manager.h"

namespace bptree {

    PageGuard::PageGuard(BufferPoolManager *bpm, Page *page, LockMode mode)
            : bpm_(bpm), page_(page), mode_(mode) {
        if (page_ == nullptr) return;
        if (mode_ == LockMode::Read) {
            page_->RLatch();
        } else if (mode_ == LockMode::Write) {
            page_->WLatch();
        }
    }

    PageGuard::~PageGuard() {
        UnlockIfHeld();
        if (page_ != nullptr) { bpm_->UnpinPage(page_->GetPageId(), is_dirty_); }
    }

    PageGuard::PageGuard(PageGuard &&other) noexcept {
        bpm_ = other.bpm_;
        page_ = other.page_;
        is_dirty_ = other.is_dirty_;
        mode_ = other.mode_;
        other.page_ = nullptr;
        other.mode_ = LockMode::None;
    }

    PageGuard &PageGuard::operator=(PageGuard &&other) noexcept {
        if (this != &other) {
            UnlockIfHeld();
            if (page_ != nullptr) { bpm_->UnpinPage(page_->GetPageId(), is_dirty_); }
            bpm_ = other.bpm_;
            page_ = other.page_;
            is_dirty_ = other.is_dirty_;
            mode_ = other.mode_;
            other.page_ = nullptr;
            other.mode_ = LockMode::None;
        }
        return *this;
    }

    void PageGuard::UnlockIfHeld() {
        if (page_ == nullptr) return;
        if (mode_ == LockMode::Read) {
            page_->RUnlatch();
        } else if (mode_ == LockMode::Write) {
            page_->WUnlatch();
        }
        mode_ = LockMode::None;
    }
}