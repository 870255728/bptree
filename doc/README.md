# B+树项目文档

## 项目目录结构
``` txt
b_plus_tree_project/
├── build/                 # 构建目录，CMake生成的所有文件都在这里，不提交到Git
├── cmake/                 # 存放CMake的辅助模块 (可选，但推荐)
│   └── format.cmake       # 例如，用于集成clang-format的模块
├── doc/                   # 文档目录
│   ├── README.md          # 本文档
│   ├── 类图.md            # 系统类图和架构设计
│   ├── 序列化.md          # 数据序列化机制说明
│   ├── 反序列化.md        # 数据反序列化机制说明
│   ├── 并发插入流程图.md  # 并发插入操作详细流程
│   ├── 并发删除流程图.md  # 并发删除操作详细流程
│   ├── 并发搜索流程图.md  # 并发搜索操作详细流程
│   ├── 并发操作总览.md    # 并发操作综合分析和对比
│   ├── 插入.png           # 插入操作流程图
│   ├── 删除.png           # 删除操作流程图
│   ├── 查找.png           # 查找操作流程图
│   └── 范围查找.png       # 范围查找操作流程图
├── include/               # 头文件目录 (.h, .hpp)
│   └── bptree/            # 为你的项目创建一个命名空间/模块
│       ├── b_plus_tree.h
│       ├── b_plus_tree_internal_page.h
│       ├── b_plus_tree_leaf_page.h
│       ├── b_plus_tree_page.h
│       ├── common.h       # 通用定义，如KeyType, ValueType, page_id_t等
│       └── exception.h    # 自定义异常类
├── src/                   # 源文件目录 (.cpp)
│   ├── b_plus_tree.cpp
│   ├── b_plus_tree_internal_page.cpp
│   ├── b_plus_tree_leaf_page.cpp
│   └── b_plus_tree_page.cpp
├── test/                  # 测试代码目录
│   ├── b_plus_tree_test.cpp  # 使用Google Test编写的单元测试
│   └── CMakeLists.txt     # 测试目录的CMake配置
├── third_party/           # 第三方库
│   └── googletest/        # Google Test框架的源码
├── .clang-format          # Clang-Format 配置文件
├── .gitignore             # Git忽略文件配置
└── CMakeLists.txt         # 项目根目录的CMake配置文件
```

## 文档说明

### 核心文档
- **[类图.md](类图.md)**: 系统整体架构和类关系图
- **[序列化.md](序列化.md)**: 数据序列化机制详细说明
- **[反序列化.md](反序列化.md)**: 数据反序列化机制详细说明

### 并发操作文档
- **[并发插入流程图.md](并发插入流程图.md)**: 多线程并发插入操作的详细执行流程
- **[并发删除流程图.md](并发删除流程图.md)**: 多线程并发删除操作的详细执行流程  
- **[并发搜索流程图.md](并发搜索流程图.md)**: 多线程并发搜索操作的详细执行流程
- **[并发操作总览.md](并发操作总览.md)**: 三种并发操作的综合分析和性能对比

### 流程图图片
- **插入.png**: 插入操作的流程图
- **删除.png**: 删除操作的流程图
- **查找.png**: 查找操作的流程图
- **范围查找.png**: 范围查找操作的流程图

## 快速导航

### 了解系统架构
1. 查看 [类图.md](类图.md) 了解整体设计
2. 查看 [序列化.md](序列化.md) 和 [反序列化.md](反序列化.md) 了解数据存储机制

### 了解并发机制
1. 查看 [并发操作总览.md](并发操作总览.md) 获得整体认识
2. 根据需要查看具体的并发操作流程图：
   - [并发插入流程图.md](并发插入流程图.md)
   - [并发删除流程图.md](并发删除流程图.md)
   - [并发搜索流程图.md](并发搜索流程图.md)

### 查看操作流程
- 查看对应的PNG图片文件了解具体的操作步骤
