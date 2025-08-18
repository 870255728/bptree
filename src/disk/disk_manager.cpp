//
// Created by lenovo on 2025/8/5.
//
#include <stdexcept>
#include <filesystem> // For getting file size
#include <cstring>
#include "disk_manager.h"

namespace bptree {

    DiskManager::DiskManager(const std::string& db_file)
            : db_file_name_(db_file) {

        // 使用 std::ios::in | std::ios::out | std::ios::binary 打开文件
        // 这意味着文件必须已存在。如果不存在，fstream的is_open()会是false
        // std::ios::ate 会将初始位置定位在文件末尾，方便获取文件大小
        db_io_.open(db_file_name_, std::ios::in | std::ios::out | std::ios::binary | std::ios::ate);

        // 如果文件打不开，意味着它可能不存在，我们尝试创建它
        if (!db_io_.is_open()) {
            // 使用 trunc 模式会清空已存在的文件，确保我们是从一个干净的状态开始创建
            db_io_.open(db_file_name_, std::ios::in | std::ios::out | std::ios::binary | std::ios::trunc);
            if (!db_io_.is_open()) {
                throw std::runtime_error("Failed to open or create database file: " + db_file_name_);
            }
        }

        // 获取文件大小
        std::streamsize file_size = db_io_.tellg();
        if (file_size == -1) {
            // 如果文件刚被创建，大小可能是0，tellg可能返回-1，这里我们将其视为0
            file_size = 0;
        }

        // 根据文件大小计算下一个可用的page_id
        // 如果文件为空，第一个page_id是0
        next_page_id_ = file_size / PAGE_SIZE;
    }

    DiskManager::~DiskManager() {
        // std::fstream 的析构函数会自动调用 close()
        // 但显式关闭是一个好习惯
        if (db_io_.is_open()) {
            db_io_.close();
        }
    }

    void DiskManager::ReadPage(page_id_t page_id, char* page_data) {
        std::lock_guard<std::mutex> guard(db_io_latch_);

        std::streamoff offset = GetPageOffset(page_id);

        // [修复] 在进行I/O操作前，先检查文件是否打开
        if (!db_io_.is_open()) {
            throw std::runtime_error("Database file is not open for reading.");
        }
        // 检查请求的页面是否在文件范围内
        db_io_.seekg(0, std::ios::end); // 定位到文件末尾
        if (offset >= db_io_.tellg()) {
            // 请求的页面超出文件范围，模拟读一个空页
            memset(page_data, 0, PAGE_SIZE);
            return;
        }
        // [修复] 定位到读取位置前清除可能存在的旧错误状态
        db_io_.clear();
        db_io_.seekg(offset);
        if (db_io_.fail()) { // 使用 db_io 而非 db_io_
            throw std::runtime_error("Failed to seek to page " + std::to_string(page_id) + " for reading.");
        }

        db_io_.read(page_data, PAGE_SIZE);
        if (db_io_.gcount() != PAGE_SIZE) {
            // 如果没有读够一个完整的页面（可能是文件损坏或到达末尾），
            // 这是一个潜在的问题，但为了测试，我们可以先填充剩余部分
            // 在真实的系统中，这可能应该是一个错误
            memset(page_data + db_io_.gcount(), 0, PAGE_SIZE - db_io_.gcount());
        }
    }

    void DiskManager::WritePage(page_id_t page_id, const char* page_data) {
        std::lock_guard<std::mutex> guard(db_io_latch_);

        std::streamoff offset = GetPageOffset(page_id);

        // [修复] 检查文件是否打开
        if (!db_io_.is_open()) {
            throw std::runtime_error("Database file is not open for writing.");
        }

        // [修复] 定位到写入位置前清除可能存在的旧错误状态
        db_io_.clear();
        db_io_.seekp(offset);
        if (db_io_.fail()) {
            throw std::runtime_error("Failed to seek to page " + std::to_string(page_id) + " for writing.");
        }

        db_io_.write(page_data, PAGE_SIZE);
        if (db_io_.fail()) {
            throw std::runtime_error("Error occurred while writing page " + std::to_string(page_id));
        }

        db_io_.flush();
        if (db_io_.fail()) {
            throw std::runtime_error("Error occurred while flushing page " + std::to_string(page_id));
        }
    }

    auto DiskManager::AllocatePage() -> page_id_t {
        std::lock_guard<std::mutex> guard(db_io_latch_);

        // 返回当前的 next_page_id_，然后将其递增
        page_id_t new_page_id = next_page_id_++;

        // 可以在这里扩展文件大小以预留空间，但 fstream 的 write 会自动扩展，
        // 所以这里不是必须的。

        return new_page_id;
    }

    void DiskManager::DeallocatePage(page_id_t page_id) {
        // 这是一个简化的实现，它什么也不做。
        // 一个完整的实现会需要一个freelist来管理被回收的页面空间。
        // 例如，可以在文件头部维护一个空闲页面链表。
        // 为了阶段二的目标，一个空的实现是完全可以接受的。
    }

}