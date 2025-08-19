//
// Created by lenovo on 2025/8/7.
//
// test/buffer_pool_manager_test.cpp

#include "gtest/gtest.h"
#include "buffer_pool_manager.h"
#include "lru_replacer.h"
#include "disk_manager.h"
#include "config.h"
#include <memory>
#include <string>
#include <random>
#include <cstdio>

namespace bptree {

// --- 测试夹具 (Test Fixture) ---
    class BufferPoolManagerTest : public ::testing::Test {
    protected:
        void SetUp() override {
            // 创建一个临时的数据库文件
            db_file_name_ = "test_bpm.db";
            remove(db_file_name_.c_str());

            // 初始化底层组件
            disk_manager_ = std::make_unique<DiskManager>(db_file_name_);
            replacer_ = std::make_unique<LRUReplacer>(POOL_SIZE); // POOL_SIZE来自config.h

            // 创建 BufferPoolManager 实例
            bpm_ = std::make_unique<BufferPoolManager>(POOL_SIZE, disk_manager_.get(), replacer_.get());
        }

        void TearDown() override {
            // 清理测试文件
            remove(db_file_name_.c_str());
        }

        std::string db_file_name_;
        std::unique_ptr<DiskManager> disk_manager_;
        std::unique_ptr<Replacer> replacer_;
        std::unique_ptr<BufferPoolManager> bpm_;
    };

// --- 测试用例 ---

// 测试1：测试 NewPage 和 FetchPage 的基本功能
    TEST_F(BufferPoolManagerTest, BasicNewAndFetch) {
        page_id_t page_id_0;

        // 1. 创建一个新页面
        Page* page_0 = bpm_->NewPage(&page_id_0);
        ASSERT_NE(page_0, nullptr);
        EXPECT_EQ(page_id_0, 0);

        // 2. 向页面写入数据
        strcpy(page_0->GetData(), "Hello");

        // 3. Unpin 页面，并标记为脏
        EXPECT_TRUE(bpm_->UnpinPage(page_id_0, true));

        // 4. 将所有页面刷回磁盘
        bpm_->FlushAllPages();

        // 5. 再次 Fetch 同一个页面
        Page* fetched_page_0 = bpm_->FetchPage(page_id_0);
        ASSERT_NE(fetched_page_0, nullptr);

        // 6. 验证数据是否一致
        EXPECT_EQ(strcmp(fetched_page_0->GetData(), "Hello"), 0);

        // 7. 清理
        EXPECT_TRUE(bpm_->UnpinPage(page_id_0, false));
    }

// 测试2：测试缓冲池满时的页面驱逐 (Eviction) 逻辑
    TEST_F(BufferPoolManagerTest, EvictionPolicy) {
        // 1. 填满缓冲池 (POOL_SIZE = 10)
        std::vector<page_id_t> page_ids;
        for (int i = 0; i < POOL_SIZE; ++i) {
            page_id_t new_page_id;
            Page* page = bpm_->NewPage(&new_page_id);
            ASSERT_NE(page, nullptr);
            page_ids.push_back(new_page_id);
            // 写入可识别的数据
            sprintf(page->GetData(), "Page %d", new_page_id);
            // 立即 Unpin，使其可以被驱逐
            EXPECT_TRUE(bpm_->UnpinPage(new_page_id, true));
        }

        // 2. 此时所有页面都在缓冲池中，并且 Replacer 中有 POOL_SIZE 个帧
        ASSERT_EQ(replacer_->Size(), POOL_SIZE);

        // 3. 创建第 11 个页面，这将触发一次驱逐
        // 根据 LRU 策略，page_id 0 应该是第一个被驱逐的
        page_id_t new_page_id_11;
        Page* page_11 = bpm_->NewPage(&new_page_id_11);
        ASSERT_NE(page_11, nullptr);

        // 4. 验证 page_id 0 确实被驱逐了
        // 尝试 Fetch 它，应该会导致一次磁盘读取 (因为我们没有模拟 DiskManager 的 IO 计数，
        // 所以我们通过检查它是否还在内存中来间接验证)
        // 此时 page_table 中不应再有 page_id 0
        Page* fetched_page_0 = bpm_->FetchPage(0);
        ASSERT_NE(fetched_page_0, nullptr);
        EXPECT_EQ(strcmp(fetched_page_0->GetData(), "Page 0"), 0);

        // 5. 清理
        bpm_->UnpinPage(0, false);
        bpm_->UnpinPage(new_page_id_11, false);
    }

// 测试3：测试被 Pin 住的页面不会被驱逐
    TEST_F(BufferPoolManagerTest, PinnedPageShouldNotBeEvicted) {
        // 1. 填满缓冲池并 Unpin 所有页面
        for (int i = 0; i < POOL_SIZE; ++i) {
            page_id_t new_page_id;
            Page* page = bpm_->NewPage(&new_page_id);
            ASSERT_NE(page, nullptr);
            EXPECT_TRUE(bpm_->UnpinPage(new_page_id, false));
        }

        // 2. 现在，重新 Fetch 并 Pin 住所有页面
        for (int i = 0; i < POOL_SIZE; ++i) {
            Page* page = bpm_->FetchPage(i);
            ASSERT_NE(page, nullptr);
            // 注意：这里没有 Unpin
        }

        // 3. 此时 Replacer 应该是空的，因为所有页面都被 Pin 住了
        ASSERT_EQ(replacer_->Size(), 0);

        // 4. 尝试创建一个新页面，应该会失败，因为没有可驱逐的帧
        page_id_t new_page_id;
        Page* new_page = bpm_->NewPage(&new_page_id);
        EXPECT_EQ(new_page, nullptr);

        // 5. 清理
        for (int i = 0; i < POOL_SIZE; ++i) {
            EXPECT_TRUE(bpm_->UnpinPage(i, false));
        }
    }

// 测试4：测试 DeletePage 功能
    TEST_F(BufferPoolManagerTest, DeletePageTest) {
        // 1. 创建一个新页面
        page_id_t page_id;
        Page* page = bpm_->NewPage(&page_id);
        ASSERT_NE(page, nullptr);
        EXPECT_EQ(page_id, 0);

        // 2. 写入数据并 Unpin
        strcpy(page->GetData(), "Data to be deleted");
        EXPECT_TRUE(bpm_->UnpinPage(page_id, true));

        // 3. 删除页面
        EXPECT_TRUE(bpm_->DeletePage(page_id));

        // 4. 再次 NewPage，应该会重用刚才被删除的页面所占用的帧
        // 并且分配一个新的 page_id
        page_id_t new_page_id;
        Page* new_page = bpm_->NewPage(&new_page_id);
        ASSERT_NE(new_page, nullptr);
        EXPECT_EQ(new_page_id, 1); // 新的 page_id 应该是 1

        // 5. 验证被删除的 page_id 无法再被 Fetch
        // 我们的 BPM 实现会重新加载它，但内容应该是空的
        Page* deleted_page = bpm_->FetchPage(page_id);
        ASSERT_NE(deleted_page, nullptr);
        // 检查内容是否被清空 (ResetMemory)
        char expected_empty_data[PAGE_SIZE] = {0};
        EXPECT_EQ(memcmp(deleted_page->GetData(), expected_empty_data, PAGE_SIZE), 0);

        // 6. 清理
        bpm_->UnpinPage(new_page_id, false);
        bpm_->UnpinPage(page_id, false);
    }


// 测试5：测试 FlushPage 和持久性
    TEST_F(BufferPoolManagerTest, PersistenceTest) {
        page_id_t page_id;
        Page* page = bpm_->NewPage(&page_id);
        ASSERT_NE(page, nullptr);
        strcpy(page->GetData(), "Persistent Data");

        // 第一次测试：Unpin 但不标记为脏
        EXPECT_TRUE(bpm_->UnpinPage(page_id, false));

        // 销毁BPM，此时不脏的页面不会被写回
        bpm_.reset();

        // 重新打开
        replacer_ = std::make_unique<LRUReplacer>(POOL_SIZE);
        bpm_ = std::make_unique<BufferPoolManager>(POOL_SIZE, disk_manager_.get(), replacer_.get());

        // 重新 Fetch，内容应该是空的（因为上次没写回）
        Page* re_fetched_page = bpm_->FetchPage(page_id);
        ASSERT_NE(re_fetched_page, nullptr); // 确保 Fetch 成功
        char expected_empty_data[PAGE_SIZE] = {0};
        EXPECT_EQ(memcmp(re_fetched_page->GetData(), expected_empty_data, PAGE_SIZE), 0);

        // 1. 在持有 Pin 的情况下修改页面
        strcpy(re_fetched_page->GetData(), "Dirty Persistent Data");

        // 2. 修改完成后，Unpin 页面并标记为脏
        EXPECT_TRUE(bpm_->UnpinPage(page_id, true));
        // 此时 re_fetched_page 指针已经失效，不能再使用

        // 3. 销毁BPM，此时脏页应该被 FlushAllPages 写回
        bpm_.reset();

        // 4. 再次重新打开
        replacer_ = std::make_unique<LRUReplacer>(POOL_SIZE);
        bpm_ = std::make_unique<BufferPoolManager>(POOL_SIZE, disk_manager_.get(), replacer_.get());

        // 5. 重新 Fetch，这次应该能读到持久化的数据
        Page* final_fetched_page = bpm_->FetchPage(page_id);
        ASSERT_NE(final_fetched_page, nullptr);
        EXPECT_EQ(strcmp(final_fetched_page->GetData(), "Dirty Persistent Data"), 0);

        // 6. 清理
        EXPECT_TRUE(bpm_->UnpinPage(page_id, false));
    }

} // namespace bptree