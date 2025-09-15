//
// Created by lenovo on 2025/8/7.
//

#ifndef BPTREE_CONFIG_H
#define BPTREE_CONFIG_H

#include <cstdint>
#include <chrono>

namespace bptree {

    static constexpr int PAGE_SIZE = 512;

    static constexpr int POOL_SIZE = 10000;

    using page_id_t = int32_t;

    using frame_id_t = int32_t;

    static constexpr page_id_t INVALID_PAGE_ID = -1;

    enum class LogLevel { INFO, WARN, ERROR, DEBUG };
    static constexpr LogLevel LOG_LEVEL = LogLevel::INFO;

    using lock_timeout_t = std::chrono::milliseconds;
    static constexpr lock_timeout_t LOCK_TIMEOUT = std::chrono::milliseconds(1000);

}

#endif //BPTREE_CONFIG_H
