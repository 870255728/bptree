//
// Created by lenovo on 2025/8/5.
//

#ifndef BPTREE_INTERNAL_NODE_H
#define BPTREE_INTERNAL_NODE_H

#include <algorithm>
#include <iterator>
#include <vector>
#include <memory>
#include "node.h"

namespace bptree {
    /**
     * @class InternalNode
     * @brief B+Tree 内部节点的页面视图类。
     *
     * 页面布局 (Layout):
     * --------------------------------------------------------------------
     * | NodeHeader |         Keys (Array)         |  Children (Array)    |
     * --------------------------------------------------------------------
     *              |<-- (max_size) * sizeof(KeyT) -->|<-- (max_size + 1) * sizeof(page_id_t) -->|
     */
    template<typename KeyT, typename ValueT, typename KeyComparator>
    class InternalNode : public Node<KeyT, ValueT, KeyComparator> {
    public:
        // 从基类继承类型别名，方便在本类和外部使用
        using Base = Node<KeyT, ValueT, KeyComparator>;
        using KeyType = typename Base::KeyType;

        void Init(char* page_data, int /*max_size*/) {
            Base::Init(page_data, false);
        }

        // --- 数据区指针获取 ---
        auto Keys_Ptr(char* page_data) -> KeyType* {
            return reinterpret_cast<KeyType*>(page_data + sizeof(NodeHeader));
        }
        auto Keys_Ptr(const char* page_data) const -> const KeyType* {
            return reinterpret_cast<const KeyType*>(page_data + sizeof(NodeHeader));
        }
        auto Children_Ptr(char* page_data, int max_size) -> page_id_t* {
            return reinterpret_cast<page_id_t*>(const_cast<char*>(reinterpret_cast<const char*>(Keys_Ptr(page_data)) + max_size * sizeof(KeyType)));
        }
        auto Children_Ptr(const char* page_data, int max_size) const -> const page_id_t* {
            return reinterpret_cast<const page_id_t*>(reinterpret_cast<const char*>(Keys_Ptr(page_data)) + max_size * sizeof(KeyType));
        }

        auto Lookup(const char* page_data, int max_size, const KeyType &key, const KeyComparator &comparator) const -> page_id_t {
            const KeyType* keys = Keys_Ptr(page_data);
            int size = this->Get_Size(page_data);
            auto it = std::upper_bound(keys, keys + size, key, comparator);
            int index = std::distance(keys, it);
            return Children_Ptr(page_data, max_size)[index];
        }

        /**
         * @brief 获取指定索引处的子节点指针
         * @param index 子节点的索引
         * @return 指向子节点的原始指针
         */
        auto Child_At(const char* page_data, int max_size, int index) const -> page_id_t {
            return Children_Ptr(page_data, max_size)[index];
        }

        // --- 修改操作 ---

        void Insert(char* page_data, int max_size, const KeyType &key, page_id_t child_page_id, const KeyComparator &comparator) {
            const KeyType* keys = Keys_Ptr(page_data);
            int size = this->Get_Size(page_data);
            auto it = std::upper_bound(keys, keys + size, key, comparator);
            int index = std::distance(keys, it);

            KeyType* mutable_keys = Keys_Ptr(page_data);
            page_id_t* mutable_children = Children_Ptr(page_data, max_size);

            memmove(&mutable_keys[index + 1], &mutable_keys[index], (size - index) * sizeof(KeyType));
            memmove(&mutable_children[index + 2], &mutable_children[index + 1], (size - index) * sizeof(page_id_t));

            mutable_keys[index] = key;
            mutable_children[index + 1] = child_page_id;

            this->Set_Size(page_data, size + 1);
        }

        auto Split(char* source_page_data, char* dest_page_data, int max_size) -> KeyType {
            int split_point = max_size / 2;
            int source_size = this->Get_Size(source_page_data);

            KeyType key_to_parent = Keys_Ptr(source_page_data)[split_point];

            int moved_keys_count = source_size - split_point - 1;
            int moved_children_count = source_size - split_point;

            KeyType* src_keys = Keys_Ptr(source_page_data) + split_point + 1;
            page_id_t* src_children = Children_Ptr(source_page_data, max_size) + split_point + 1;
            KeyType* dest_keys = Keys_Ptr(dest_page_data);
            page_id_t* dest_children = Children_Ptr(dest_page_data, max_size);

            memcpy(dest_keys, src_keys, moved_keys_count * sizeof(KeyType));
            memcpy(dest_children, src_children, moved_children_count * sizeof(page_id_t));

            this->Set_Size(source_page_data, split_point);
            this->Set_Size(dest_page_data, moved_keys_count);

            return key_to_parent;
        }

        void Populate_New_Root(char* page_data, int max_size, const KeyType &key, page_id_t left_child_id, page_id_t right_child_id) {
            this->Set_Size(page_data, 1);
            Keys_Ptr(page_data)[0] = key;
            Children_Ptr(page_data, max_size)[0] = left_child_id;
            Children_Ptr(page_data, max_size)[1] = right_child_id;
        }

        /**
         * @brief 查找一个子节点指针在 children_ 数组中的索引
         */
        auto Find_Child_Index(const char* page_data, int max_size, page_id_t child_id) const -> int {
            int size = this->Get_Size(page_data);
            const page_id_t* children = Children_Ptr(page_data, max_size);
            for (int i = 0; i <= size; ++i) {
                if (children[i] == child_id) {
                    return i;
                }
            }
            return -1; // Not found
        }

        /**
         * @brief 移除指定索引处的键和子节点指针
         */
        void Remove_At(char* page_data, int max_size, int key_index) {
            int size = this->Get_Size(page_data);
            KeyType* keys = Keys_Ptr(page_data);
            page_id_t* children = Children_Ptr(page_data, max_size);

            memmove(&keys[key_index], &keys[key_index + 1], (size - key_index - 1) * sizeof(KeyType));
            // 移除键右侧的子节点
            memmove(&children[key_index + 1], &children[key_index + 2], (size - key_index - 1) * sizeof(page_id_t));

            this->Set_Size(page_data, size - 1);
        }

        /**
         * @brief 设置指定索引处的键
         */
        void Set_Key_At(char* page_data, int index, const KeyType& key) {
            Keys_Ptr(page_data)[index] = key;
        }

        /**
         * @brief 将左兄弟的最后一个元素(键+子指针)移动到本节点开头
         * @param sibling 左兄弟节点
         * @param parent 父节点
         * @param parent_key_index 父节点中分隔本节点和左兄弟的键的索引
         */
        void Move_Last_From(char* current_page_data, char* sibling_page_data, int max_size, char* parent_page_data, int parent_key_index) {
            int sibling_size = this->Get_Size(sibling_page_data);
            int current_size = this->Get_Size(current_page_data);

            KeyType key_from_parent = Keys_Ptr(parent_page_data)[parent_key_index];
            page_id_t child_from_sibling = Children_Ptr(sibling_page_data, max_size)[sibling_size];

            memmove(Keys_Ptr(current_page_data) + 1, Keys_Ptr(current_page_data), current_size * sizeof(KeyType));
            memmove(Children_Ptr(current_page_data, max_size) + 1, Children_Ptr(current_page_data, max_size), (current_size + 1) * sizeof(page_id_t));

            Keys_Ptr(current_page_data)[0] = key_from_parent;
            Children_Ptr(current_page_data, max_size)[0] = child_from_sibling;

            Set_Key_At(parent_page_data, parent_key_index, Keys_Ptr(sibling_page_data)[sibling_size - 1]);

            this->Set_Size(current_page_data, current_size + 1);
            this->Set_Size(sibling_page_data, sibling_size - 1);
        }

        /**
         * @brief 将右兄弟的第一个元素(键+子指针)移动到本节点末尾
         * @param sibling 右兄弟节点
         * @param parent 父节点
         * @param parent_key_index 父节点中分隔本节点和右兄弟的键的索引
         */
        void Move_First_From(char* current_page_data, char* sibling_page_data, int max_size, char* parent_page_data, int parent_key_index) {
            int current_size = this->Get_Size(current_page_data);

            KeyType key_from_parent = Keys_Ptr(parent_page_data)[parent_key_index];
            page_id_t child_from_sibling = Children_Ptr(sibling_page_data, max_size)[0];

            Keys_Ptr(current_page_data)[current_size] = key_from_parent;
            Children_Ptr(current_page_data, max_size)[current_size + 1] = child_from_sibling;

            Set_Key_At(parent_page_data, parent_key_index, Keys_Ptr(sibling_page_data)[0]);

            int sibling_size = this->Get_Size(sibling_page_data);
            memmove(Keys_Ptr(sibling_page_data), Keys_Ptr(sibling_page_data) + 1, (sibling_size - 1) * sizeof(KeyType));
            memmove(Children_Ptr(sibling_page_data, max_size), Children_Ptr(sibling_page_data, max_size) + 1, sibling_size * sizeof(page_id_t));

            this->Set_Size(current_page_data, current_size + 1);
            this->Set_Size(sibling_page_data, sibling_size - 1);
        }

        /**
         * @brief 将右兄弟的所有数据合并到本节点
         * @param sibling 右兄弟节点
         * @param parent_key_index 父节点中分隔键的索引
         * @param parent 父节点
         */
        void Merge_Into(char* current_page_data, char* sibling_page_data, int max_size, char* parent_page_data, int parent_key_index) {
            int current_size = this->Get_Size(current_page_data);
            int sibling_size = this->Get_Size(sibling_page_data);

            KeyType key_from_parent = Keys_Ptr(parent_page_data)[parent_key_index];
            Keys_Ptr(current_page_data)[current_size] = key_from_parent;

            memcpy(Keys_Ptr(current_page_data) + current_size + 1, Keys_Ptr(sibling_page_data), sibling_size * sizeof(KeyType));
            memcpy(Children_Ptr(current_page_data, max_size) + current_size + 1, Children_Ptr(sibling_page_data, max_size), (sibling_size + 1) * sizeof(page_id_t));

            this->Set_Size(current_page_data, current_size + sibling_size + 1);
        }

        /**
         * @brief 移除并返回第一个子节点的所有权。
         * 这个函数主要用于当根节点下溢，树的高度需要降低时。
         */
        auto Move_First_Child(char* page_data, int max_size) -> page_id_t {
            return Children_Ptr(page_data, max_size)[0];
        }

//    private:
//        std::vector<ChildPtr> children_;
    };
}

#endif //BPTREE_INTERNAL_NODE_H
