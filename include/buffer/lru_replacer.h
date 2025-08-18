//
// Created by lenovo on 2025/8/18.
//

#pragma once

#include "replacer.h"
#include <list>
#include <unordered_map>
#include <mutex>

namespace bptree {

    /**
     * @class LRUReplacer
     * @brief LRUReplacer 实现了基于最近最少使用（LRU）策略的页面替换算法。
     *
     * 它继承自 Replacer 抽象基-类，并使用一个双向链表和一个哈希表来
     * 高效地跟踪可替换的帧。
     */
    class LRUReplacer : public Replacer {
    public:
        /**
         * @brief 构造函数。
         * @param num_frames 缓冲池中的总帧数（即缓冲池的大小）。
         */
        explicit LRUReplacer(size_t num_frames);

        /**
         * @brief 析构函数。
         */
        ~LRUReplacer() override;

        // 禁用拷贝和赋值操作。
        LRUReplacer(const LRUReplacer&) = delete;
        LRUReplacer& operator=(const LRUReplacer&) = delete;

        /**
         * @brief 从 LRU 列表中选择最近最少使用的帧作为受害者。
         * @param[out] frame_id 指向用于存储受害者帧ID的指针。
         * @return 如果成功找到受害者，则返回 true；否则返回 false。
         */
        auto Victim(frame_id_t* frame_id) -> bool override;

        /**
         * @brief 固定一个帧，将其从 LRU 列表中移除。
         * @param frame_id 要固定的帧的ID。
         */
        void Pin(frame_id_t frame_id) override;

        /**
         * @brief 解除固定一个帧，将其添加到 LRU 列表的头部（表示最近使用）。
         * @param frame_id 要解除固定的帧的ID。
         */
        void Unpin(frame_id_t frame_id) override;

        /**
         * @brief 返回 LRU 列表中可替换帧的数量。
         * @return 可替换帧的数量。
         */
        auto Size() const -> size_t override;

    private:
        // 使用双向链表来维护 LRU 顺序。头部是 MRU (Most Recently Used)，尾部是 LRU。
        std::list<frame_id_t> lru_list_;

        // 使用哈希表来快速查找 lru_list_ 中的节点，实现 O(1) 的 Pin 操作。
        // key: frame_id, value: 指向 lru_list_ 中对应节点的迭代器
        std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> lru_map_;

        // 用于保护内部数据结构的互斥锁，确保线程安全。
        mutable std::mutex latch_;

        // 缓冲池的大小
        size_t capacity_;
    };

} // namespace bptree
