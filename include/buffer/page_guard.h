// include/buffer/page_guard.h

#pragma once

// --- [核心修改 1: 使用前向声明] ---
// 我们只需要知道 BufferPoolManager 是一个类，来声明一个指针。
// #include "buffer/buffer_pool_manager.h" // <--- 移除这行
#include "page.h" // 仍然需要 Page 的完整定义

namespace bptree {

// 前向声明 BufferPoolManager 类
    class BufferPoolManager;

    class PageGuard {
    public:
        // 构造函数
        PageGuard(BufferPoolManager* bpm, Page* page);

        // 析构函数
        ~PageGuard(); // <--- 只保留声明

        // 移动构造函数
        PageGuard(PageGuard&& other) noexcept;

        // 移动赋值运算符
        PageGuard& operator=(PageGuard&& other) noexcept;

        // ... (其他内联函数保持不变) ...
        // 获取页面数据指针
        auto GetData() -> char* { return page_ != nullptr ? page_->GetData() : nullptr; }
        auto GetData() const -> const char* { return page_ != nullptr ? page_->GetData() : nullptr; }
        // 获取页面ID
        auto GetPageId() const -> page_id_t { return page_ != nullptr ? page_->GetPageId() : INVALID_PAGE_ID; }
        // 将页面标记为脏
        void SetDirty() { is_dirty_ = true; }
        // 判断 guard 是否有效
        explicit operator bool() const { return page_ != nullptr; }

        // 禁止拷贝
        PageGuard(const PageGuard&) = delete;
        PageGuard& operator=(const PageGuard&) = delete;

    private:
        BufferPoolManager* bpm_;
        Page* page_;
        bool is_dirty_ = false;
    };

} // namespace bptree