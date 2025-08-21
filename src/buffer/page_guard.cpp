//
// Created by lenovo on 2025/8/20.
//
// src/buffer/page_guard.cpp

#include "page_guard.h"
#include "buffer_pool_manager.h" // <--- 在这里包含完整定义

namespace bptree {

// 构造函数的实现
    PageGuard::PageGuard(BufferPoolManager* bpm, Page* page)
            : bpm_(bpm), page_(page) {}

// 析构函数的实现
    PageGuard::~PageGuard() {
        // 在这里，因为包含了 buffer_pool_manager.h，
        // 所以 BufferPoolManager 是一个完整类型，可以调用其成员函数。
        if (page_ != nullptr) {
            bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
        }
    }

// 移动构造函数的实现
    PageGuard::PageGuard(PageGuard&& other) noexcept {
        bpm_ = other.bpm_;
        page_ = other.page_;
        is_dirty_ = other.is_dirty_;
        other.page_ = nullptr;
    }

// 移动赋值运算符的实现
    PageGuard& PageGuard::operator=(PageGuard&& other) noexcept {
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

} // namespace bptree