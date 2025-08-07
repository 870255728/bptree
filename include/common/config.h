//
// Created by lenovo on 2025/8/7.
//

#ifndef BPTREE_CONFIG_H
#define BPTREE_CONFIG_H

#include <cstdint>
#include <chrono>

namespace bptree {

// --- 存储与页面配置 (Storage & Page Configuration) ---

// 页面大小（字节）。4KB是数据库系统中非常常见的大小。
// 设置为 static constexpr 确保它是一个编译期常量。
    static constexpr int PAGE_SIZE = 4096;

// 缓冲池中帧（Frames）的数量。
// 这个值决定了缓冲池的内存大小 (POOL_SIZE * PAGE_SIZE)。
// 在测试时可以设置得小一些，以便更容易地测试页面替换逻辑。
    static constexpr int POOL_SIZE = 10;

// --- ID 类型别名 (ID Type Aliases) ---

// 页面ID的类型。使用有符号整数，以便用 -1 表示无效ID。
    using page_id_t = int32_t;

// 帧ID的类型（缓冲池中帧的索引）。
    using frame_id_t = int32_t;

// --- 常量 (Constants) ---

// 定义一个表示无效页面ID的常量。
    static constexpr page_id_t INVALID_PAGE_ID = -1;

// --- 日志与调试配置 (Logging & Debugging) ---

// (可选) 你可以在这里添加日志相关的配置
// 例如，日志级别
    enum class LogLevel { INFO, WARN, ERROR, DEBUG };
    static constexpr LogLevel LOG_LEVEL = LogLevel::INFO;

// --- 并发控制配置 (Concurrency Control) ---
// (阶段三会用到)

// 锁管理器的等待超时时间
    using lock_timeout_t = std::chrono::milliseconds;
    static constexpr lock_timeout_t LOCK_TIMEOUT = std::chrono::milliseconds(1000);

}

#endif //BPTREE_CONFIG_H
