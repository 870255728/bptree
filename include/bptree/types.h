//
// Created by lenovo on 2025/8/5.
//

#ifndef BPTREE_TYPES_H
#define BPTREE_TYPES_H

#include <cstdint>
#include <functional>

namespace bptree {

    using page_id_t = int32_t;
    using frame_id_t = int32_t;

    constexpr page_id_t INVALID_PAGE_ID = -1;
}

#endif //BPTREE_TYPES_H
