//
// Created by lenovo on 2025/8/14.
//
#include <iostream>
#include <vector>
#include <random>
#include <algorithm>
#include <chrono>
#include "b_plus_tree.h"

// 使用 using 别名让代码更简洁
using Clock = std::chrono::high_resolution_clock;
using TimePoint = std::chrono::time_point<Clock>;
using Duration = std::chrono::duration<double>;

// --- 辅助函数：生成随机数据 ---
std::vector<int> generate_random_keys(int count) {
    std::vector<int> keys(count);
    // 使用 std::iota 快速生成 0, 1, 2, ...
    std::iota(keys.begin(), keys.end(), 0);

    // 使用 Mersenne Twister 引擎进行高质量的随机打乱
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(keys.begin(), keys.end(), g);

    return keys;
}

// --- 测试函数：插入性能 ---
void benchmark_insert(int order, int num_keys) {
    std::cout << "--- Benchmarking INSERT with Order = " << order
              << ", NumKeys = " << num_keys << " ---" << std::endl;

    auto keys = generate_random_keys(num_keys);
    bptree::BPlusTree<int, int> tree(order, order);

    TimePoint start = Clock::now();
    for (int key : keys) {
        tree.Insert(key, key);
    }
    TimePoint end = Clock::now();

    Duration elapsed = end - start;
    std::cout << "Total time: " << elapsed.count() << " seconds" << std::endl;
    std::cout << "Throughput: " << num_keys / elapsed.count() << " inserts/sec" << std::endl;
    std::cout << std::endl;
}

// --- 测试函数：查找性能 ---
void benchmark_get(int order, int num_keys) {
    std::cout << "--- Benchmarking GET with Order = " << order
              << ", NumKeys = " << num_keys << " ---" << std::endl;

    auto keys_to_insert = generate_random_keys(num_keys);
    bptree::BPlusTree<int, int> tree(order, order);
    for (int key : keys_to_insert) {
        tree.Insert(key, key);
    }

    auto keys_to_find = generate_random_keys(num_keys);
    int value;

    TimePoint start = Clock::now();
    for (int key : keys_to_find) {
        tree.Get_Value(key, &value);
    }
    TimePoint end = Clock::now();

    Duration elapsed = end - start;
    std::cout << "Total time: " << elapsed.count() << " seconds" << std::endl;
    std::cout << "Throughput: " << num_keys / elapsed.count() << " gets/sec" << std::endl;
    std::cout << std::endl;
}

// --- 测试函数：范围扫描性能 ---
void benchmark_scan(int order, int num_keys, int scan_size) {
    std::cout << "--- Benchmarking SCAN with Order = " << order
              << ", NumKeys = " << num_keys << ", ScanSize = " << scan_size << " ---" << std::endl;

    bptree::BPlusTree<int, int> tree(order, order);
    for (int i = 0; i < num_keys; ++i) {
        tree.Insert(i, i); // 顺序插入，便于范围扫描
    }

    TimePoint start = Clock::now();
    auto it = tree.begin();
    for(int i = 0; i < scan_size; ++i) {
        // 解引用操作来模拟访问
        [[maybe_unused]] auto pair = *it;
        ++it;
    }
    TimePoint end = Clock::now();

    Duration elapsed = end - start;
    std::cout << "Total time: " << elapsed.count() << " seconds" << std::endl;
    std::cout << "Throughput: " << scan_size / elapsed.count() << " scans/sec" << std::endl;
    std::cout << std::endl;
}


int main(int argc, char** argv) {
    // 定义测试参数
    const int NUM_KEYS = 100000; // 数据量
    const std::vector<int> ORDERS = {4, 8, 16, 32, 64, 128, 256, 512}; // 不同的度数

    std::cout << "====== Performance Benchmark Start ======" << std::endl;

    // 运行插入测试
    for (int order : ORDERS) {
        benchmark_insert(order, NUM_KEYS);
    }

    // 运行查找测试
    for (int order : ORDERS) {
        benchmark_get(order, NUM_KEYS);
    }

    // 运行扫描测试
    for (int order : ORDERS) {
        benchmark_scan(order, NUM_KEYS, NUM_KEYS / 10); // 扫描10%的数据
    }

    std::cout << "====== Performance Benchmark End ======" << std::endl;

    return 0;
}