#ifndef BPTREE_REPLACER_H
#define BPTREE_REPLACER_H

#include <cstdlib>
#include "config.h"

namespace bptree {
    class Replacer {
    public:
        Replacer() = default;

        virtual ~Replacer() = default;

        Replacer(const Replacer &) = delete;

        Replacer &operator=(const Replacer &) = delete;

        /**
         * @brief 从buffer pool选择一个帧，并将其替换掉
         */
        virtual auto Victim(frame_id_t* frameId) -> bool = 0;

        /**
         * @brief 固定缓冲池中的某一帧
         * 当一个页面的 pin_count 从 0 增加到 1 时，缓冲池管理器会调用此函数。
         * 这个帧应该从可替换列表中移除。
         * @param frame_id 要固定的帧的ID。
         */
        virtual void Pin(frame_id_t frameId) = 0;

        /**
         * @brief 取消固定，说明需要被替换掉
         *
         */
        virtual void Unpin(frame_id_t frameId) = 0;

        /**
         * @brief 缓冲池帧的数量
         * @return 帧的数量
         */
        virtual auto Size() const ->  size_t = 0;
    };
}


#endif //BPTREE_REPLACER_H
