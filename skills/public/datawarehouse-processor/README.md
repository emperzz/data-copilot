# 数仓SQL处理器技能 (datawarehouse-processor)

## 技能简介

这是一个专门处理数仓相关SQL任务的技能，能够帮助用户完成以下工作：

1. **SQL语法检查和错误修复** - 检查SQL语法错误并提供修复建议
2. **SQL性能优化** - 分析SQL性能问题并提供优化建议
3. **元数据提取和存储** - 提取SQL的元数据并存储到数仓目录
4. **SQL方言转换** - 将SQL转换为不同的数据库方言
5. **SQL生成** - 根据业务需求生成SQL查询
6. **字段定义查询** - 查询表结构、字段定义和血缘关系

## 技能文件结构

```
datawarehouse-processor/
├── SKILL.md                    # 技能主文件（核心逻辑和说明）
├── evals/                      # 测试用例目录
│   └── evals.json              # 测试用例配置
├── benchmark.md                # 测试报告
├── benchmark.json              # 测试结果（JSON格式）
├── optimization_notes.md       # 优化说明
└── README.md                   # 本文件
```

## 使用方法

### 1. 安装技能

将 `datawarehouse-processor` 目录复制到你的技能目录中：

```bash
cp -r datawarehouse-processor /path/to/skills/
```

### 2. 触发技能

技能会在以下场景自动触发：

- 提到"数仓"、"数据仓库"、"SQL查询"、"SQL优化"、"SQL错误检查"
- 提到"元数据查询"、"血缘分析"、"字段定义"、"生成SQL"
- 提到"SQL转换"、"方言迁移"、"SQL语法验证"、"性能优化"
- 提供SQL代码并要求检查、优化或分析

### 3. 使用示例

#### 示例1：SQL语法检查

**输入：**
```
这个SQL报错了，帮我检查一下语法：
SELECT * FROM orders WHERE create_time = '2026-03-12' AND status = ''
```

**输出：**
```
## SQL错误分析

### 错误类型：语法错误
**位置：** WHERE子句
**原因：** status的值是空字符串，可能导致意外的结果

### 解决方案
检查status字段的值是否正确，如果是要查询status为空的记录，应该使用：
WHERE status IS NULL 或 WHERE status = ''

### 修复后的SQL
```sql
SELECT * FROM orders WHERE create_time = '2026-03-12' AND status = 'active'
```
```

#### 示例2：SQL优化

**输入：**
```
帮我优化这个SQL，它运行太慢了：
SELECT * FROM orders, users WHERE orders.user_id = users.id AND orders.create_time > '2026-03-01'
```

**输出：**
```
## SQL优化分析

### 性能问题识别
- 使用了隐式JOIN（FROM orders, users WHERE ...）
- 使用了SELECT *，可能返回不必要的字段

### 优化建议
1. 将隐式JOIN改为显式JOIN
2. 只选择必要的字段
3. 在user_id和create_time字段上创建索引

### 优化后的SQL
```sql
SELECT orders.id, orders.user_id, users.name, orders.create_time
FROM orders
INNER JOIN users ON orders.user_id = users.id
WHERE orders.create_time > '2026-03-01'
```

### 预期改进
- 使用显式JOIN可以提高查询效率
- 只选择必要字段可以减少数据传输量
- 添加索引可以加速查询
```

#### 示例3：元数据提取和存储

**输入：**
```
帮我分析这个SQL的元数据并存储到数仓目录：
WITH daily_sales AS (
  SELECT date, SUM(amount) as total
  FROM orders
  GROUP BY date
)
SELECT * FROM daily_sales WHERE total > 1000
```

**输出：**
```
## SQL元数据提取

### 提取的元数据
- **表名：** orders
- **字段：** date, amount, total
- **CTE：** daily_sales
- **语句类型：** SELECT

### 存储结果
- **Ingest ID：** 96dc33d0-183b-439d-aae6-7ff490a4710c
- **状态：** 成功
- **存储的语句数：** 2

### 语句说明
1. CTE daily_sales：按日期汇总订单金额
2. 主查询：筛选汇总金额大于1000的日期
```

#### 示例4：SQL方言转换

**输入：**
```
把这个SQL转换成MySQL语法：
SELECT substr(name, 1, 10) FROM users WHERE id != 0
```

**输出：**
```
## SQL方言转换

### 源方言：通用SQL
### 目标方言：MySQL

### 转换后的SQL
```sql
SELECT SUBSTRING(name, 1, 10) FROM users WHERE id <> 0
```

### 主要变更点
- substr() → SUBSTRING()
- != → <>
```

#### 示例5：生成SQL

**输入：**
```
帮我生成一个SQL，查询最近7天的订单数量，按天分组，只统计已支付的订单
```

**输出：**
```
## 生成的SQL查询

### 查询说明
查询最近7天内已支付订单的数量，按日期分组统计

### SQL
```sql
SELECT
  DATE(create_time) as order_date,
  COUNT(DISTINCT order_id) as order_count
FROM orders
WHERE create_time >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  AND status = 'paid'
GROUP BY DATE(create_time)
ORDER BY order_date
```

### 使用的表和字段
- **表名：** orders - 订单表
- **字段：** create_time - 订单创建时间
- **字段：** order_id - 订单ID
- **字段：** status - 订单状态
```

#### 示例6：查询字段定义

**输入：**
```
字段device_id是什么意思？它的数据来源是什么？
```

**输出：**
```
## device_id 定义

### 基本信息
- **名称：** device_id
- **类型：** 字段
- **所属表：** user_events

### 定义说明
device_id是用户设备的唯一标识符，用于标识用户的设备（手机、平板、电脑等）

### 数据来源和血缘
- **来源表：** user_events
- **血缘路径：** user_events.device_id → dwd_user_device_di.device_id → ods_user_device.device_id

### 计算逻辑
device_id直接从客户端上报的事件中获取，不做任何转换

### 相关字段
- user_id - 用户ID
- device_type - 设备类型（手机/平板/电脑）
- os_version - 操作系统版本
```

## 内置工具

技能使用以下内置工具：

1. **sql_check_syntax** - 检查SQL语法
2. **sql_transpile** - 转换SQL方言
3. **sql_extract_metadata** - 提取SQL元数据
4. **dw_catalog_ingest_sql** - 存储SQL和元数据到数仓目录

## 测试结果

### 测试覆盖率
- 测试用例总数：8个
- 核心功能测试：3个
- 通过率：100%

### 测试场景
1. ✅ SQL语法检查
2. ✅ SQL优化
3. ✅ 元数据提取和存储
4. ✅ SQL方言转换
5. ✅ SQL生成
6. ✅ 字段定义查询
7. ✅ SQL语法验证
8. ✅ 表结构查询

详细测试报告请查看 `benchmark.md` 文件。

## 版本信息

- **技能名称：** datawarehouse-processor
- **版本：** iteration-1
- **创建日期：** 2026-03-30
- **最后更新：** 2026-03-30

## 贡献者

技能由DeerFlow团队创建和维护。

## 许可证

本技能遵循开源许可证。

## 支持

如有问题或建议，请联系技能维护团队。
