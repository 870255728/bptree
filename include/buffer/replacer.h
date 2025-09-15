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

        virtual auto Victim(frame_id_t* frameId) -> bool = 0;

        virtual void Pin(frame_id_t frameId) = 0;

        virtual void Unpin(frame_id_t frameId) = 0;

        virtual auto Size() const ->  size_t = 0;
    };
}


#endif //BPTREE_REPLACER_H
