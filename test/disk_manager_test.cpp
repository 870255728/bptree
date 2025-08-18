//
// Created by lenovo on 2025/8/7.
//
#include "gtest/gtest.h"
#include "disk_manager.h"
#include "config.h"
#include <string>
#include <vector>
#include <cstring>
#include <cstdio>

// --- 测试套件：DiskManagerTest ---
// 这个套件将测试DiskManager的所有核心功能

class DiskManagerTest : public ::testing::Test {
protected:
    // 每个测试用例开始前都会调用SetUp()
    void SetUp() override {
        // 创建一个临时的、唯一的数据库文件名用于测试
        db_file_name_ = "test_disk_manager.db";
        // 在每个测试开始前，确保旧的测试文件被删除，保证测试环境的纯净
        remove(db_file_name_.c_str());
        // 创建DiskManager实例
        disk_manager_ = new bptree::DiskManager(db_file_name_);
    }

    // 每个测试用例结束后都会调用TearDown()
    void TearDown() override {
        delete disk_manager_;
        // 测试结束后，清理掉测试文件
        remove(db_file_name_.c_str());
    }

    std::string db_file_name_;
    bptree::DiskManager* disk_manager_;
};

// 测试1：测试页面的基本读写功能
TEST_F(DiskManagerTest, SimpleReadWriteTest) {
    // 准备要写入的数据
    char write_buffer[bptree::PAGE_SIZE];
    memset(write_buffer, 'A', sizeof(write_buffer));

    // 写入第0页
    bptree::page_id_t page_id = 0;
    disk_manager_->WritePage(page_id, write_buffer);

    // 准备一个用于读取的缓冲区
    char read_buffer[bptree::PAGE_SIZE];
    memset(read_buffer, 0, sizeof(read_buffer)); // 清空以确保读取的是文件内容

    // 从第0页读回数据
    disk_manager_->ReadPage(page_id, read_buffer);

    // 验证读回的数据与写入的数据完全一致
    // memcmp 比较两块内存区域，如果相同则返回0
    EXPECT_EQ(memcmp(write_buffer, read_buffer, bptree::PAGE_SIZE), 0);

    // 修改写入缓冲区的内容，再次写入
    memset(write_buffer, 'B', sizeof(write_buffer));
    disk_manager_->WritePage(page_id, write_buffer);

    // 再次读回并验证
    disk_manager_->ReadPage(page_id, read_buffer);
    EXPECT_EQ(memcmp(write_buffer, read_buffer, bptree::PAGE_SIZE), 0);
}

// 测试2：测试多页面的读写
TEST_F(DiskManagerTest, MultiplePageReadWriteTest) {
    std::vector<char> write_buffers(5 * bptree::PAGE_SIZE);

    // 准备5个不同的页面数据
    for (int i = 0; i < 5; ++i) {
        memset(write_buffers.data() + i * bptree::PAGE_SIZE, 'A' + i, bptree::PAGE_SIZE);
    }

    // 将5个页面写入磁盘
    for (int i = 0; i < 5; ++i) {
        disk_manager_->WritePage(i, write_buffers.data() + i * bptree::PAGE_SIZE);
    }

    // 准备读取缓冲区
    char read_buffer[bptree::PAGE_SIZE];

    // 逐一读回并验证每个页面的内容
    for (int i = 0; i < 5; ++i) {
        memset(read_buffer, 0, sizeof(read_buffer));
        disk_manager_->ReadPage(i, read_buffer);
        EXPECT_EQ(memcmp(write_buffers.data() + i * bptree::PAGE_SIZE, read_buffer, bptree::PAGE_SIZE), 0);
    }
}


// 测试3：测试页面分配功能
TEST_F(DiskManagerTest, AllocatePageTest) {
    // 初始状态下，新创建的文件应该从 page_id 0 开始分配
    EXPECT_EQ(disk_manager_->AllocatePage(), 0);
    EXPECT_EQ(disk_manager_->AllocatePage(), 1);
    EXPECT_EQ(disk_manager_->AllocatePage(), 2);

    // 写入一些数据到分配的页面
    char buffer[bptree::PAGE_SIZE];
    memset(buffer, 'X', sizeof(buffer));
    disk_manager_->WritePage(0, buffer);
    disk_manager_->WritePage(1, buffer);
    disk_manager_->WritePage(2, buffer);
}

// 测试4：测试重新打开文件后的状态
TEST_F(DiskManagerTest, ReopenFileTest) {
    // 1. 在当前的 disk_manager_ 实例中分配并写入页面
    disk_manager_->AllocatePage(); // page 0
    disk_manager_->AllocatePage(); // page 1

    char write_buffer[bptree::PAGE_SIZE];
    memset(write_buffer, 'Z', sizeof(write_buffer));
    disk_manager_->WritePage(1, write_buffer);

    // 2. 销毁当前实例（TearDown会自动调用delete，但我们在这里手动模拟）
    delete disk_manager_;

    // 3. 重新创建一个新的 DiskManager 实例，打开同一个文件
    disk_manager_ = new bptree::DiskManager(db_file_name_);

    // 4. 验证新实例的状态
    // 由于之前分配了2个页面(0, 1)，下一个分配的页面ID应该是2
    EXPECT_EQ(disk_manager_->AllocatePage(), 2);

    // 5. 验证之前写入的数据是否仍然存在
    char read_buffer[bptree::PAGE_SIZE];
    disk_manager_->ReadPage(1, read_buffer);
    EXPECT_EQ(memcmp(write_buffer, read_buffer, bptree::PAGE_SIZE), 0);
}