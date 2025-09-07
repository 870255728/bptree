//
// Created by lenovo on 2025/8/18.
//
#include "lru_replacer.h"
#include <iostream>

namespace bptree {

    LRUReplacer::LRUReplacer(size_t num_frames) : capacity_(num_frames) {}

    LRUReplacer::~LRUReplacer() = default;

    auto LRUReplacer::Victim(frame_id_t* frame_id) -> bool {
        // 加锁以保护共享数据结构
        std::lock_guard<std::mutex> guard(latch_);

        // 如果 LRU 列表为空，则没有可替换的帧
        if (lru_list_.empty()) {
            std::cout << "[LRU] Victim: LRU列表为空，没有可替换的页面" << std::endl;
            return false;
        }

        // 选择列表末尾的帧作为受害者 (Least Recently Used)
        *frame_id = lru_list_.back();

        // 输出当前LRU列表状态
        std::cout << "[LRU] Victim: 选择页面 " << *frame_id << " 作为置换受害者" << std::endl;
        std::cout << "[LRU] 当前LRU列表状态: ";
        for (auto it = lru_list_.begin(); it != lru_list_.end(); ++it) {
            std::cout << *it;
            if (std::next(it) != lru_list_.end()) {
                std::cout << " -> ";
            }
        }
        std::cout << " (头部=最近使用，尾部=最久未使用)" << std::endl;

        // 从列表和哈希表中移除受害者帧
        lru_map_.erase(*frame_id);
        lru_list_.pop_back();

        std::cout << "[LRU] 页面 " << *frame_id << " 已从LRU列表中移除" << std::endl;
        std::cout << "[LRU] 剩余可替换页面数量: " << lru_list_.size() << std::endl;

        return true;
    }

    void LRUReplacer::Pin(frame_id_t frame_id) {
        // 加锁以保护共享数据结构
        std::lock_guard<std::mutex> guard(latch_);

        std::cout << "[LRU] Pin: 尝试固定页面 " << frame_id << std::endl;

        // 检查该帧是否存在于可替换列表中
        auto it = lru_map_.find(frame_id);
        if (it != lru_map_.end()) {
            // 如果存在，说明它当前是可替换的，现在被 Pin 了，
            // 需要从 LRU 列表中移除。
            std::cout << "[LRU] 页面 " << frame_id << " 在LRU列表中，正在移除..." << std::endl;
            lru_list_.erase(it->second); // it->second 是存储的 list 迭代器
            lru_map_.erase(it);
            std::cout << "[LRU] 页面 " << frame_id << " 已固定，从LRU列表中移除" << std::endl;
            std::cout << "[LRU] 剩余可替换页面数量: " << lru_list_.size() << std::endl;
        } else {
            std::cout << "[LRU] 页面 " << frame_id << " 不在LRU列表中，可能已经被固定" << std::endl;
        }
    }

    void LRUReplacer::Unpin(frame_id_t frame_id) {
        // 加锁以保护共享数据结构
        std::lock_guard<std::mutex> guard(latch_);

        std::cout << "[LRU] Unpin: 尝试解除固定页面 " << frame_id << std::endl;

        // 首先检查该帧是否已经在 LRU 列表中
        // 如果已经在，说明一个 unpinned 的帧被再次 unpin，这通常不应该发生，
        // 但我们的实现可以优雅地处理它：先移除旧的，再插入新的。
        // 但更简单的做法是假设调用者保证了 Pin/Unpin 的正确性，
        // 并且一个 frame 不会 unpin 两次。
        if (lru_map_.count(frame_id)) {
            // 帧已经在可替换列表中，无需操作
            std::cout << "[LRU] 页面 " << frame_id << " 已经在LRU列表中，无需重复添加" << std::endl;
            return;
        }

        // 检查是否超出容量限制 (可选，但良好实践)
        // 理论上，可替换帧的数量不应超过缓冲池大小
        if (lru_list_.size() >= capacity_) {
            // 可以选择在这里记录一个警告，因为这可能表示一个逻辑错误
            // 但为了简单，我们允许它继续，因为 Victim 会处理列表
            std::cout << "[LRU] 警告: 缓冲池超出容量限制! 当前大小: " << lru_list_.size() 
                      << ", 容量: " << capacity_ << std::endl;
        }

        // 将该帧添加到 LRU 列表的前端 (表示最近被 unpin)
        lru_list_.push_front(frame_id);
        // 在哈希表中存储该帧在列表中的位置（迭代器）
        lru_map_[frame_id] = lru_list_.begin();

        std::cout << "[LRU] 页面 " << frame_id << " 已添加到LRU列表头部 (最近使用)" << std::endl;
        std::cout << "[LRU] 当前LRU列表状态: ";
        for (auto it = lru_list_.begin(); it != lru_list_.end(); ++it) {
            std::cout << *it;
            if (std::next(it) != lru_list_.end()) {
                std::cout << " -> ";
            }
        }
        std::cout << " (头部=最近使用，尾部=最久未使用)" << std::endl;
        std::cout << "[LRU] 当前可替换页面数量: " << lru_list_.size() << std::endl;
    }

    auto LRUReplacer::Size() const -> size_t {
        // 加锁以保护 lru_list_
        std::lock_guard<std::mutex> guard(latch_);
        std::cout << "[LRU] Size: 查询当前可替换页面数量 = " << lru_list_.size() << std::endl;
        return lru_list_.size();
    }

} // namespace bptree