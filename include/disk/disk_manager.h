//
// Created by lenovo on 2025/8/5.
//

#ifndef BPTREE_DISK_MANAGER_H
#define BPTREE_DISK_MANAGER_H

#include <string>
#include <fstream>
#include <mutex>
#include "config.h"

namespace bptree {

/**
 * @class DiskManager
 * @brief 负责与磁盘文件进行底层的页面读写交互。
 *
 * DiskManager 将数据库文件抽象为一个由固定大小页面组成的数组。
 * 它提供按页面ID读写数据的接口，并管理页面的分配。
 * 这个类是线程安全的。
 */
    class DiskManager {
    public:
        /**
         * @brief 构造函数。
         *
         * 如果指定的数据库文件不存在，则会创建一个新文件。
         *
         * @param db_file 数据库文件的路径。
         */
        explicit DiskManager(const std::string& db_file);

        /**
         * @brief 析构函数。
         *
         * 自动关闭文件流。
         */
        ~DiskManager();

        /**
         * @brief 将指定页面的内容从磁盘读入内存缓冲区。
         *
         * @param page_id 要读取的页面ID。
         * @param[out] page_data 指向内存缓冲区的指针，大小至少为 PAGE_SIZE。
         */
        void ReadPage(page_id_t page_id, char* page_data);

        /**
         * @brief 将内存缓冲区的内容写入磁盘上的指定页面。
         *
         * @param page_id 要写入的页面ID。
         * @param page_data 指向内存缓冲区的指针，大小为 PAGE_SIZE。
         */
        void WritePage(page_id_t page_id, const char* page_data);

        /**
         * @brief 在数据库文件中分配一个新页面。
         *
         * @return 分配的新页面的ID。
         */
        auto AllocatePage() -> page_id_t;

        /**
         * @brief 释放一个页面。
         *
         * (注意：在简单实现中，此函数可能为空操作，不实际回收空间)
         *
         * @param page_id 要释放的页面ID。
         */
        void DeallocatePage(page_id_t page_id);

        // 禁用拷贝构造和拷贝赋值，因为DiskManager管理着一个唯一的文件资源。
        DiskManager(const DiskManager&) = delete;
        DiskManager& operator=(const DiskManager&) = delete;

    private:
        /**
         * @brief 计算给定页面ID在文件中的偏移量。
         */
        auto GetPageOffset(page_id_t page_id) const -> std::streamoff {
            return static_cast<std::streamoff>(page_id) * PAGE_SIZE;
        }

        // 数据库文件名
        std::string db_file_name_;

        // C++ 文件流对象，用于读写数据库文件
        std::fstream db_io_;

        // 用于保护文件I/O操作的互斥锁，确保线程安全
        std::mutex db_io_latch_;

        // 用于分配新页面ID的计数器
        page_id_t next_page_id_;
    };

}

#endif //BPTREE_DISK_MANAGER_H
