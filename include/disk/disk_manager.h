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

    class DiskManager {
    public:
        explicit DiskManager(const std::string& db_file);

        ~DiskManager();

        DiskManager(const DiskManager&) = delete;
        DiskManager& operator=(const DiskManager&) = delete;

        void ReadPage(page_id_t page_id, char* page_data);

        void WritePage(page_id_t page_id, const char* page_data);

        auto AllocatePage() -> page_id_t;

        void DeallocatePage(page_id_t page_id);

    private:
        auto GetPageOffset(page_id_t page_id) const -> std::streamoff {
            return static_cast<std::streamoff>(page_id) * PAGE_SIZE;
        }

        std::string db_file_name_;

        std::fstream db_io_;

        std::mutex db_io_latch_;

        page_id_t next_page_id_;
    };

}

#endif //BPTREE_DISK_MANAGER_H
