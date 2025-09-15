//
// Created by lenovo on 2025/8/5.
//

#ifndef BPTREE_LEAF_NODE_H
#define BPTREE_LEAF_NODE_H

#include <utility> // For std::pair
#include <vector>
#include <iterator>
#include <algorithm>
#include <memory>
#include <iostream>
#include "node.h"
#include "internal_node.h"

namespace bptree {
    /**
     * @class LeafNode
     * @brief B+Tree 叶子节点的页面视图类。
     *
     * 页面布局 (Layout):
     * -------------------------------------------------------------------------------------
     * | NodeHeader | NextPageID |         Keys (Array)         |       Values (Array)        |
     * -------------------------------------------------------------------------------------
     */
    template<class KeyT, class ValueT, class KeyComparator>
    class LeafNode : public Node<KeyT, ValueT, KeyComparator> {
    public:

        using InternalNodeT = InternalNode<KeyT, ValueT, KeyComparator>;

        // 从基类继承类型别名，方便在本类和外部使用
        using Base = Node<KeyT, ValueT, KeyComparator>;
        using KeyType = typename Base::KeyType;
        using ValueType = typename Base::ValueType;

        void Init(char* page_data, int max_size) {
            Base::Init(page_data, true);
            Set_Next_Page_Id(page_data, INVALID_PAGE_ID);
        }

        // --- 元数据访问 ---
        auto Get_Next_Page_Id(const char* page_data) const -> page_id_t {
            return *reinterpret_cast<const page_id_t*>(page_data + sizeof(NodeHeader));
        }

        void Set_Next_Page_Id(char* page_data, page_id_t next_page_id) {
            *reinterpret_cast<page_id_t*>(page_data + sizeof(NodeHeader)) = next_page_id;
        }

        // --- 数据区指针获取 ---
        auto Keys_Ptr(char* page_data) -> KeyType* {
            return reinterpret_cast<KeyType*>(page_data + sizeof(NodeHeader) + sizeof(page_id_t));
        }
        auto Keys_Ptr(const char* page_data) const -> const KeyType* {
            return reinterpret_cast<const KeyType*>(page_data + sizeof(NodeHeader) + sizeof(page_id_t));
        }
        auto Values_Ptr(char* page_data, int max_size) -> ValueType* {
            return reinterpret_cast<ValueType*>(reinterpret_cast<char*>(Keys_Ptr(page_data)) + max_size * sizeof(KeyType));
        }
        auto Values_Ptr(const char* page_data, int max_size) const -> const ValueType* {
            return reinterpret_cast<const ValueType*>(reinterpret_cast<const char*>(Keys_Ptr(page_data)) + max_size * sizeof(KeyType));
        }

        auto Get_Value(const char* page_data, int max_size, const KeyType &key, ValueType *value, const KeyComparator &comparator) const -> bool {
            int index = Find_Key_Index(page_data, max_size, key, comparator);
            int size = this->Get_Size(page_data);

            // 先获取 keys 数组的头指针，然后再用 [index] 访问
            if (index < size && !comparator(key, Keys_Ptr(page_data)[index]) && !comparator(Keys_Ptr(page_data)[index], key)) {
                *value = Value_At(page_data, max_size, index);
                return true;
            }
            return false;
        }

        auto Value_At(const char* page_data, int max_size, int index) const -> const ValueType& {
            return Values_Ptr(page_data, max_size)[index];
        }
        auto Value_At(char* page_data, int max_size, int index) -> ValueType& {
            return Values_Ptr(page_data, max_size)[index];
        }

        void Insert(char* page_data, int max_size, const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
            int index = Find_Key_Index(page_data, max_size, key, comparator);
            int size = this->Get_Size(page_data);

            KeyType* keys = Keys_Ptr(page_data);
            ValueType* values = Values_Ptr(page_data, max_size);

            // 如果键已存在，则不进行插入（保持size不变）
            if (index < size && !comparator(key, keys[index]) && !comparator(keys[index], key)) {
                return;
            }

            memmove(&keys[index + 1], &keys[index], (size - index) * sizeof(KeyType));
            memmove(&values[index + 1], &values[index], (size - index) * sizeof(ValueType));

            keys[index] = key;
            values[index] = value;

            this->Set_Size(page_data, size + 1);
            // debug dump
            std::cout << "[LEAF][INS] idx=" << index << " size=" << this->Get_Size(page_data) << " keys=";
            for (int i = 0; i < this->Get_Size(page_data); i++) {
                std::cout << (i==0?"":" ") << keys[i];
            }
            std::cout << " values=";
            for (int i = 0; i < this->Get_Size(page_data); i++) {
                std::cout << (i==0?"":" ") << values[i];
            }
            std::cout << std::endl;
        }

        void Remove(char* page_data, int max_size, const KeyType &key, const KeyComparator &comparator) {
            int index = Find_Key_Index(page_data, max_size, key, comparator);
            int size = this->Get_Size(page_data);

            if (index < size && !comparator(key, Keys_Ptr(page_data)[index]) && !comparator(Keys_Ptr(page_data)[index], key)) {
                KeyType* keys = Keys_Ptr(page_data);
                ValueType* values = Values_Ptr(page_data, max_size);

                memmove(&keys[index], &keys[index + 1], (size - index - 1) * sizeof(KeyType));
                memmove(&values[index], &values[index + 1], (size - index - 1) * sizeof(ValueType));

                this->Set_Size(page_data, size - 1);
            }
        }

        auto Split(char* source_page_data, char* dest_page_data, int max_size) -> KeyType {
            int source_size = this->Get_Size(source_page_data);
            int split_point = source_size / 2; // use current size, not capacity
            int moved_count = source_size - split_point;

            KeyType* src_keys = Keys_Ptr(source_page_data) + split_point;
            ValueType* src_values = Values_Ptr(source_page_data, max_size) + split_point;
            KeyType* dest_keys = Keys_Ptr(dest_page_data);
            ValueType* dest_values = Values_Ptr(dest_page_data, max_size);

            std::cout << "[LEAF][SPLIT] source_size=" << source_size
                      << " split_point=" << split_point
                      << " moved_count=" << moved_count << std::endl;
            std::cout << "[LEAF][SPLIT][SRC] keys=";
            for (int i = 0; i < source_size; i++) {
                std::cout << (i==0?"":" ") << Keys_Ptr(source_page_data)[i];
            }
            std::cout << " values=";
            for (int i = 0; i < source_size; i++) {
                std::cout << (i==0?"":" ") << Values_Ptr(source_page_data, max_size)[i];
            }
            std::cout << std::endl;

            memcpy(dest_keys, src_keys, moved_count * sizeof(KeyType));
            memcpy(dest_values, src_values, moved_count * sizeof(ValueType));

            this->Set_Size(source_page_data, split_point);
            this->Set_Size(dest_page_data, moved_count);

            // debug dump after move
            std::cout << "[LEAF][SPLIT][AFTER] left_size=" << this->Get_Size(source_page_data)
                      << " right_size=" << this->Get_Size(dest_page_data) << std::endl;
            std::cout << "[LEAF][LEFT] keys=";
            for (int i = 0; i < this->Get_Size(source_page_data); i++) {
                std::cout << (i==0?"":" ") << Keys_Ptr(source_page_data)[i];
            }
            std::cout << " values=";
            for (int i = 0; i < this->Get_Size(source_page_data); i++) {
                std::cout << (i==0?"":" ") << Values_Ptr(source_page_data, max_size)[i];
            }
            std::cout << std::endl;
            std::cout << "[LEAF][RIGHT] keys=";
            for (int i = 0; i < this->Get_Size(dest_page_data); i++) {
                std::cout << (i==0?"":" ") << dest_keys[i];
            }
            std::cout << " values=";
            for (int i = 0; i < this->Get_Size(dest_page_data); i++) {
                std::cout << (i==0?"":" ") << dest_values[i];
            }
            std::cout << std::endl;

            return dest_keys[0];
        }

        void Move_Last_From(char* current_page_data, char* sibling_page_data, int max_size) {
            int sibling_size = this->Get_Size(sibling_page_data);
            int current_size = this->Get_Size(current_page_data);

            // 1. 获取要移动的键值对
            KeyType borrowed_key = Keys_Ptr(sibling_page_data)[sibling_size - 1];
            ValueType borrowed_value = Values_Ptr(sibling_page_data, max_size)[sibling_size - 1];

            // 2. 在当前节点开头腾出空间
            memmove(Keys_Ptr(current_page_data) + 1, Keys_Ptr(current_page_data), current_size * sizeof(KeyType));
            memmove(Values_Ptr(current_page_data, max_size) + 1, Values_Ptr(current_page_data, max_size), current_size * sizeof(ValueType));

            // 3. 插入新元素
            Keys_Ptr(current_page_data)[0] = borrowed_key;
            Values_Ptr(current_page_data, max_size)[0] = borrowed_value;

            // 4. 更新大小
            this->Set_Size(sibling_page_data, sibling_size - 1);
            this->Set_Size(current_page_data, current_size + 1);
        }

        void Move_First_From(char* current_page_data, char* sibling_page_data, int max_size) {
            int current_size = this->Get_Size(current_page_data);

            // 1. 直接将兄弟节点的第一个元素追加到当前节点末尾
            Keys_Ptr(current_page_data)[current_size] = Keys_Ptr(sibling_page_data)[0];
            Values_Ptr(current_page_data, max_size)[current_size] = Values_Ptr(sibling_page_data, max_size)[0];

            // 2. 从兄弟节点中移除第一个元素
            int sibling_size = this->Get_Size(sibling_page_data);
            memmove(Keys_Ptr(sibling_page_data), Keys_Ptr(sibling_page_data) + 1, (sibling_size - 1) * sizeof(KeyType));
            memmove(Values_Ptr(sibling_page_data, max_size), Values_Ptr(sibling_page_data, max_size) + 1, (sibling_size - 1) * sizeof(ValueType));

            // 3. 更新大小
            this->Set_Size(current_page_data, current_size + 1);
            this->Set_Size(sibling_page_data, sibling_size - 1);
        }

        void Merge(char* current_page_data, char* sibling_page_data, int max_size) {
            int current_size = this->Get_Size(current_page_data);
            int sibling_size = this->Get_Size(sibling_page_data);

            KeyType* dest_keys = Keys_Ptr(current_page_data) + current_size;
            ValueType* dest_values = Values_Ptr(current_page_data, max_size) + current_size;
            KeyType* src_keys = Keys_Ptr(sibling_page_data);
            ValueType* src_values = Values_Ptr(sibling_page_data, max_size);

            memcpy(dest_keys, src_keys, sibling_size * sizeof(KeyType));
            memcpy(dest_values, src_values, sibling_size * sizeof(ValueType));

            this->Set_Size(current_page_data, current_size + sibling_size);
            Set_Next_Page_Id(current_page_data, Get_Next_Page_Id(sibling_page_data));
        }

        auto Find_Key_Index(const char* page_data, int max_size, const KeyType &key, const KeyComparator &comparator) const -> int {
            const KeyType* keys = Keys_Ptr(page_data);
            int size = this->Get_Size(page_data);
            auto it = std::lower_bound(keys, keys + size, key, comparator);
            int idx = std::distance(keys, it);
            std::cout << "[LEAF][IDX] key=" << key << " size=" << size << " -> idx=" << idx << std::endl;
            return idx;
        }
    };
}
#endif
