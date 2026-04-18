# Mini HTTP Client - 分支使用指南

## 📁 分支说明

这个项目有两个主要分支：

### 1. `2.6` 分支（学习版）

**用途**：自己动手实践和学习

**包含内容**：
- ✅ 完整的项目代码
- ✅ 三份学习文档：
  - `README.md` - 完整文档
  - `QUICKSTART.md` - 5分钟快速开始
  - `LEARNING_GUIDE.md` - 与 Kafka 源码对比
- ✅ 可运行的 Demo
- ❌ **不包含**答案和详细讲解

**适合**：
- 想自己动手实现的学习者
- 想挑战扩展练习的开发者
- 想深入理解设计原理的读者

**使用方法**：
```bash
# 克隆仓库
git clone git@github.com:TinaTong-coder/kafka.git
cd kafka

# 切换到学习分支
git checkout 2.6

# 进入项目
cd mini-http-client

# 阅读文档
cat QUICKSTART.md

# 编译运行
./build.sh
./run.sh

# 尝试扩展练习（答案在 answer 分支）
# 1. 添加 POST 请求支持
# 2. 添加超时处理
# 3. 实现重试机制
# ...
```

---

### 2. `answer` 分支（答案版）

**用途**：查看答案、学习最佳实践、理解设计原理

**包含内容**：
- ✅ `2.6` 分支的所有内容
- ✅ **ANSWERS.md** - 核心问题的深度答案：
  - 为什么单线程？
  - 为什么用 compose()？
  - 递归调用如何处理？
  - 回调顺序如何保证？
  - ... 等 20+ 个问题

- ✅ **EXERCISE_SOLUTIONS.md** - 扩展练习的完整答案：
  - 初级：POST、超时、重试
  - 中级：连接池、取消、同步 API
  - 高级：HTTPS、优先级队列

- ✅ **RequestFutureAnnotated.java** - 逐行注释版本：
  - 每一行代码的作用
  - 设计决策的原因
  - 性能和线程安全考虑
  - 与 Kafka 的对比

**适合**：
- 遇到困难需要参考答案
- 想了解最佳实践
- 想深入理解每行代码的作用

**使用方法**：
```bash
# 切换到答案分支
git checkout answer

cd mini-http-client

# 查看核心问题答案
cat ANSWERS.md

# 查看练习题答案
cat EXERCISE_SOLUTIONS.md

# 查看详细注释版代码
cat src/main/java/com/example/future/RequestFutureAnnotated.java
```

---

## 🎯 推荐学习路径

### 路径 1: 自学挑战（推荐）

```
1. checkout 2.6 分支
   ↓
2. 阅读 QUICKSTART.md
   ↓
3. 运行 Demo，观察输出
   ↓
4. 阅读核心代码（RequestFuture、NetworkClient、HttpClient）
   ↓
5. 尝试自己完成扩展练习
   ↓
6. 遇到困难时，checkout answer 分支查看答案
   ↓
7. 对比自己的实现和答案版本
   ↓
8. 回到 2.6 分支，优化自己的代码
```

### 路径 2: 快速学习

```
1. checkout 2.6 分支
   ↓
2. 运行 Demo
   ↓
3. checkout answer 分支
   ↓
4. 阅读 ANSWERS.md 理解核心概念
   ↓
5. 阅读 RequestFutureAnnotated.java 理解实现细节
   ↓
6. 对比 Kafka 源码（LEARNING_GUIDE.md）
```

### 路径 3: 深度研究

```
1. checkout answer 分支
   ↓
2. 完整阅读三份答案文档
   ↓
3. 逐个实现扩展练习，参考答案优化
   ↓
4. 阅读 Kafka 源码，找到对应部分
   ↓
5. 写自己的异步客户端项目
```

---

## 📊 分支对比表

| 内容 | 2.6 分支 | answer 分支 |
|-----|---------|------------|
| 完整代码 | ✅ | ✅ |
| 学习文档 | ✅ | ✅ |
| 可运行 Demo | ✅ | ✅ |
| 核心问题答案 | ❌ | ✅ |
| 练习题答案 | ❌ | ✅ |
| 详细代码注释 | ❌ | ✅ |
| 设计决策讲解 | ❌ | ✅ |
| 最佳实践示例 | ❌ | ✅ |

---

## 🔗 快速访问

### 2.6 分支（学习版）
- **GitHub**: https://github.com/TinaTong-coder/kafka/tree/2.6/mini-http-client
- **克隆**: `git clone -b 2.6 git@github.com:TinaTong-coder/kafka.git`

### answer 分支（答案版）
- **GitHub**: https://github.com/TinaTong-coder/kafka/tree/answer/mini-http-client
- **克隆**: `git clone -b answer git@github.com:TinaTong-coder/kafka.git`

---

## 💡 使用建议

### 初学者
1. 从 `2.6` 分支开始
2. 先运行 Demo，观察输出
3. 阅读 QUICKSTART.md
4. 遇到困难时查看 `answer` 分支

### 进阶开发者
1. 从 `2.6` 分支开始
2. 尝试自己完成所有练习
3. 对比 `answer` 分支的实现
4. 找出差距，理解最佳实践

### Kafka 源码研究者
1. 直接看 `answer` 分支
2. 阅读 LEARNING_GUIDE.md
3. 对比 Kafka 源码
4. 理解设计模式的应用

---

## 📚 文档导航

### 2.6 分支文档
```
mini-http-client/
├── README.md              # 完整文档
├── QUICKSTART.md          # 5分钟快速开始
├── LEARNING_GUIDE.md      # 与 Kafka 源码对比
└── BRANCH_GUIDE.md        # 本文件
```

### answer 分支额外文档
```
mini-http-client/
├── ANSWERS.md                           # 🔥 核心问题答案
├── EXERCISE_SOLUTIONS.md                # 🔥 练习题完整答案
└── src/main/java/com/example/future/
    └── RequestFutureAnnotated.java      # 🔥 逐行注释版本
```

---

## 🤔 常见问题

**Q: 应该先看哪个分支？**
A: 建议从 `2.6` 分支开始，先自己尝试理解代码，遇到困难再看 `answer` 分支。

**Q: 答案分支会更新吗？**
A: 会的！如果有新的练习题或问题，会持续更新 `answer` 分支。

**Q: 可以直接用答案分支的代码吗？**
A: 可以，但建议先理解设计原理，然后根据自己的需求调整。

**Q: 如何切换分支？**
A:
```bash
# 切换到学习分支
git checkout 2.6

# 切换到答案分支
git checkout answer

# 查看当前分支
git branch
```

**Q: 两个分支可以对比吗？**
A:
```bash
# 查看两个分支的差异
git diff 2.6 answer

# 只看某个文件的差异
git diff 2.6 answer -- mini-http-client/ANSWERS.md
```

---

## 📖 推荐阅读顺序

1. **QUICKSTART.md** (5 分钟) - 快速了解项目
2. **README.md** (30 分钟) - 理解完整架构
3. **运行 Demo** (10 分钟) - 观察实际运行
4. **LEARNING_GUIDE.md** (2 小时) - 对比 Kafka 源码
5. **ANSWERS.md** (2 小时) - 深入理解设计
6. **EXERCISE_SOLUTIONS.md** (4 小时) - 学习最佳实践
7. **RequestFutureAnnotated.java** (1 小时) - 理解每行代码

**总学习时间**：约 10-15 小时

---

**祝学习愉快！🎉**

有任何问题，欢迎提 Issue：https://github.com/TinaTong-coder/kafka/issues
