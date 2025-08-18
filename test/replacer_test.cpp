//
// Created by lenovo on 2025/8/7.
//
#include "gtest/gtest.h"
#include "lru_replacer.h"
#include <memory>

namespace bptree {

// 使用 TEST_F 宏来创建一个测试夹具 (Test Fixture),
// 这样我们可以在多个测试用例之间共享设置代码。
    class LRUReplacerTest : public ::testing::Test {
    protected:
        // 每个测试用例开始前都会调用 SetUp()
        void SetUp() override {
            // 创建一个容量为 10 的 LRUReplacer 实例
            lru_replacer_ = std::make_unique<LRUReplacer>(10);
        }

        // 每个测试用例结束后都会调用 TearDown()
        void TearDown() override {
            // 可以在这里进行清理工作，但 unique_ptr 会自动处理
        }

        // 指向 LRUReplacer 的智能指针
        std::unique_ptr<LRUReplacer> lru_replacer_;
    };

/**
 * @brief 测试基本的 Unpin 和 Victim 操作
 */
    TEST_F(LRUReplacerTest, BasicUnpinAndVictim) {
        // 1. Unpin 几个帧
        lru_replacer_->Unpin(1);
        lru_replacer_->Unpin(2);
        lru_replacer_->Unpin(3);
        lru_replacer_->Unpin(4);
        lru_replacer_->Unpin(5);

        // 2. 验证 Size
        ASSERT_EQ(5, lru_replacer_->Size());

        // 3. Victim 操作应按 Unpin 的相反顺序移除帧 (FIFO for Unpin)
        frame_id_t victim_frame;
        bool result = lru_replacer_->Victim(&victim_frame);
        ASSERT_TRUE(result);
        EXPECT_EQ(1, victim_frame); // 1 是最先 unpin 的, 所以是 LRU

        result = lru_replacer_->Victim(&victim_frame);
        ASSERT_TRUE(result);
        EXPECT_EQ(2, victim_frame);

        result = lru_replacer_->Victim(&victim_frame);
        ASSERT_TRUE(result);
        EXPECT_EQ(3, victim_frame);

        // 4. 验证 Size 已经更新
        ASSERT_EQ(2, lru_replacer_->Size());
    }

/**
 * @brief 测试 Pin 操作
 */
    TEST_F(LRUReplacerTest, PinOperation) {
        // 1. Unpin 几个帧
        lru_replacer_->Unpin(1);
        lru_replacer_->Unpin(2);
        lru_replacer_->Unpin(3);

        ASSERT_EQ(3, lru_replacer_->Size());

        // 2. Pin 一个在 replacer 中的帧
        lru_replacer_->Pin(2);
        ASSERT_EQ(2, lru_replacer_->Size());

        // 3. Victim 操作应该跳过被 Pin 的帧
        frame_id_t victim_frame;
        lru_replacer_->Victim(&victim_frame);
        EXPECT_EQ(1, victim_frame); // 2 被 pin 了, 所以 1 是下一个受害者

        lru_replacer_->Victim(&victim_frame);
        EXPECT_EQ(3, victim_frame);

        // 4. Pin 一个不在 replacer 中的帧，应该没有效果
        lru_replacer_->Pin(4);
        ASSERT_EQ(0, lru_replacer_->Size());

        // 5. 现在 replacer 应该是空的
        bool result = lru_replacer_->Victim(&victim_frame);
        ASSERT_FALSE(result);
    }

/**
 * @brief 测试 LRU 规则的核心：访问一个 unpinned 的帧会更新其位置
 */
    TEST_F(LRUReplacerTest, LRUBehavior) {
        // 1. Unpin 序列: 1 -> 2 -> 3 -> 4
        // LRU 列表 (从尾到头, LRU -> MRU): [1, 2, 3, 4]
        lru_replacer_->Unpin(1);
        lru_replacer_->Unpin(2);
        lru_replacer_->Unpin(3);
        lru_replacer_->Unpin(4);

        // 2. "访问" 帧 2 (通过 Pin 再 Unpin 模拟)
        // 这应该将 2 移动到 MRU (most recently used) 的位置
        lru_replacer_->Pin(2);
        lru_replacer_->Unpin(2);
        // LRU 列表现在应该是: [1, 3, 4, 2]

        // 3. 验证 Victim 顺序
        frame_id_t victim_frame;
        lru_replacer_->Victim(&victim_frame);
        EXPECT_EQ(1, victim_frame); // 1 仍然是 LRU

        lru_replacer_->Victim(&victim_frame);
        EXPECT_EQ(3, victim_frame); // 然后是 3

        lru_replacer_->Victim(&victim_frame);
        EXPECT_EQ(4, victim_frame); // 然后是 4

        lru_replacer_->Victim(&victim_frame);
        EXPECT_EQ(2, victim_frame); // 2 是最后被访问的, 所以是最后一个受害者
    }

/**
 * @brief 测试边界条件
 */
    TEST_F(LRUReplacerTest, EdgeCases) {
        frame_id_t victim_frame;

        // 1. 对空 replacer 调用 Victim
        ASSERT_FALSE(lru_replacer_->Victim(&victim_frame));
        ASSERT_EQ(0, lru_replacer_->Size());

        // 2. 重复 Unpin 同一个帧
        lru_replacer_->Unpin(1);
        lru_replacer_->Unpin(1);
        lru_replacer_->Unpin(1);
        ASSERT_EQ(1, lru_replacer_->Size()); // Size 应该仍然是 1

        // 3. 重复 Pin 同一个帧
        lru_replacer_->Pin(1);
        lru_replacer_->Pin(1);
        ASSERT_EQ(0, lru_replacer_->Size()); // Size 应该变为 0

        // 4. 填满 replacer
        for (int i = 0; i < 10; ++i) {
            lru_replacer_->Unpin(i);
        }
        ASSERT_EQ(10, lru_replacer_->Size());

        // 5. 尝试 Unpin 超出容量的帧
        // 我们的实现允许这样做，因为容量只是一个逻辑上限
        lru_replacer_->Unpin(10);
        ASSERT_EQ(11, lru_replacer_->Size()); // Size 会增加

        // 验证第一个受害者是 0
        lru_replacer_->Victim(&victim_frame);
        EXPECT_EQ(0, victim_frame);
    }

} // namespace bptree