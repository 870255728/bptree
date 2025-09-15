#pragma once

#include "replacer.h"
#include <list>
#include <unordered_map>
#include <mutex>

namespace bptree {

    class LRUReplacer : public Replacer {
    public:
        explicit LRUReplacer(size_t num_frames);

        ~LRUReplacer() override;

        LRUReplacer(const LRUReplacer&) = delete;
        LRUReplacer& operator=(const LRUReplacer&) = delete;

        auto Victim(frame_id_t* frame_id) -> bool override;

        void Pin(frame_id_t frame_id) override;

        void Unpin(frame_id_t frame_id) override;

        auto Size() const -> size_t override;

    private:
        std::list<frame_id_t> lru_list_;

        std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> lru_map_;

        mutable std::mutex latch_;

        size_t capacity_;
    };

}
