项目目录结构
``` txt
b_plus_tree_project/
├── build/                 # 构建目录，CMake生成的所有文件都在这里，不提交到Git
├── cmake/                 # 存放CMake的辅助模块 (可选，但推荐)
│   └── format.cmake       # 例如，用于集成clang-format的模块
├── doc/                   # 文档目录
│   └── report.md          # 你的项目报告
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
