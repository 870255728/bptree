#include "page_guard.h"
#include "buffer_pool_manager.h"

namespace bptree {

    PageGuard::PageGuard(BufferPoolManager *bpm, Page *page, LatchMode mode)
            : bpm_(bpm), page_(page), mode_(mode) {
        if (page_ != nullptr) {
            if (mode_ == LatchMode::Read) {
                page_->RLatch();
            } else if (mode_ == LatchMode::Write) {
                page_->WLatch();
            }
        }
    }

    PageGuard::~PageGuard() {
        if (page_ != nullptr) {
            // Unlock before unpin to avoid unlocking a repurposed frame
            if (mode_ == LatchMode::Read) {
                page_->RUnlatch();
            } else if (mode_ == LatchMode::Write) {
                page_->WUnlatch();
            }
            bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
        }
    }

    PageGuard::PageGuard(PageGuard &&other) noexcept {
        bpm_ = other.bpm_;
        page_ = other.page_;
        is_dirty_ = other.is_dirty_;
        mode_ = other.mode_;
        other.page_ = nullptr;
    }

    PageGuard &PageGuard::operator=(PageGuard &&other) noexcept {
        if (this != &other) {
            if (page_ != nullptr) {
                if (mode_ == LatchMode::Read) {
                    page_->RUnlatch();
                } else if (mode_ == LatchMode::Write) {
                    page_->WUnlatch();
                }
                bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
            }
            bpm_ = other.bpm_;
            page_ = other.page_;
            is_dirty_ = other.is_dirty_;
            mode_ = other.mode_;
            other.page_ = nullptr;
        }
        return *this;
    }

    void PageGuard::Drop() {
        if (page_ == nullptr) return;
        if (mode_ == LatchMode::Read) {
            page_->RUnlatch();
        } else if (mode_ == LatchMode::Write) {
            page_->WUnlatch();
        }
        bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
        page_ = nullptr;
        bpm_ = nullptr;
        is_dirty_ = false;
        mode_ = LatchMode::None;
    }
}