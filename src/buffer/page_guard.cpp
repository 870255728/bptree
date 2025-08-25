#include "page_guard.h"
#include "buffer_pool_manager.h"

namespace bptree {

    PageGuard::PageGuard(BufferPoolManager *bpm, Page *page)
            : bpm_(bpm), page_(page) {}

    PageGuard::PageGuard(BufferPoolManager *bpm, Page *page, LatchMode latch_mode)
            : bpm_(bpm), page_(page), latch_mode_(latch_mode) {}

    PageGuard::~PageGuard() {
        if (page_ != nullptr) {
            // Release latch first, then unpin
            if (latch_mode_ == LatchMode::Read) {
                page_->ReleaseReadLatch();
            } else if (latch_mode_ == LatchMode::Write) {
                page_->ReleaseWriteLatch();
            }
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
                if (latch_mode_ == LatchMode::Read) {
                    page_->ReleaseReadLatch();
                } else if (latch_mode_ == LatchMode::Write) {
                    page_->ReleaseWriteLatch();
                }
                bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
            }
            bpm_ = other.bpm_;
            page_ = other.page_;
            is_dirty_ = other.is_dirty_;
            latch_mode_ = other.latch_mode_;
            other.page_ = nullptr;
        }
        return *this;
    }

    void PageGuard::Drop() {
        if (page_ == nullptr) {
            return;
        }
        if (latch_mode_ == LatchMode::Read) {
            page_->ReleaseReadLatch();
        } else if (latch_mode_ == LatchMode::Write) {
            page_->ReleaseWriteLatch();
        }
        bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
        page_ = nullptr;
        latch_mode_ = LatchMode::None;
        is_dirty_ = false;
    }
}