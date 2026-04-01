# 快速安装指南

## 安装步骤

### 1. 复制技能文件

将 `datawarehouse-processor` 目录复制到你的技能目录中：

```bash
# 假设你的技能目录是 /mnt/skills
cp -r datawarehouse-processor /mnt/skills/
```

### 2. 验证安装

检查技能是否正确安装：

```bash
ls -la /mnt/skills/datawarehouse-processor/
```

你应该看到以下文件：
- SKILL.md
- evals/evals.json
- benchmark.md
- benchmark.json
- optimization_notes.md
- README.md
- INSTALL.md (本文件)

### 3. 测试技能

尝试使用技能：

**测试1：SQL语法检查**
```
这个：SQL报错了，帮我检查一下语法：SELECT * FROM orders WHERE create_time = '2026-03-12'
```

**测试2：SQL优化**
```
帮我优化这个SQL：SELECT * FROM orders, users WHERE orders.user_id = users.id
```

**测试3：元数据提取**
```
帮我分析这个SQL的元数据：SELECT * FROM orders WHERE status = 'active'
```

## 卸载

如果需要卸载技能：

```bash
rm -rf /mnt/skills/datawarehouse-processor
```

## 更新

如果需要更新技能：

```bash
# 备份旧版本
cp -r /mnt/skills/datawarehouse-processor /mnt/skills/datawarehouse-processor.backup

# 复制新版本
cp -r datawarehouse-processor /mnt/skills/
```

## 常见问题

### Q: 技能没有自动触发？

A: 请确保你的输入包含以下关键词之一：
- 数仓、数据仓库、SQL查询、SQL优化、SQL错误检查
- 元数据查询、血缘分析、字段定义、生成SQL
- SQL转换、方言迁移、SQL语法验证、性能优化

或者你的输入中包含SQL代码。

### Q: 如何查看技能的详细说明？

A: 查看 `README.md` 文件获取详细的使用说明和示例。

### Q: 如何运行测试？

A: 查看测试报告 `benchmark.md` 了解测试结果。

## 支持

如有问题，请查看 `README.md` 或联系技能维护团队。
