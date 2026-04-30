# Enterprise Structured Memory System — Design

**Date**: 2026-04-28
**Status**: Approved

## Context

不同企业有不同的数据仓库框架、库表约束、数据定义，不同数据表或字段对应不同的业务定义。这些信息需要持久化记忆，让 agent 能在后续任务中主动利用。现有 `memory.json` 系统面向个人偏好/上下文，不适用于企业级结构化知识管理。

## Architecture

```
┌─────────────────────────────────────────────────┐
│                 Agent System                     │
│  ┌──────────────┐  ┌──────────────────────────┐ │
│  │ System Prompt │  │    Built-in Tools          │ │
│  │ <structured_  │  │  search_structured_memory  │ │
│  │  memory>      │  │  get_memory_entity         │ │
│  │ (index only)  │  │  list_memory_entities      │ │
│  │              │  │  update_memory_index        │ │
│  └──────────────┘  └──────────────────────────┘ │
│                                                    │
│  ┌──────────────────────────────────────────────┐ │
│  │  Skill: structured-memory/SKILL.md             │ │
│  │  定义：判断逻辑、分类规范、更新工作流              │ │
│  └──────────────────────────────────────────────┘ │
└──────────────────────┬───────────────────────────┘
                       │
┌──────────────────────▼───────────────────────────┐
│     Structured Memory Store                       │
│     {base_dir}/structured_memory/                 │
│     ├── README.md                                 │
│     ├── facts/                                    │
│     │   ├── index.md                              │
│     │   ├── schema/    (数仓架构)                   │
│     │   └── business/  (业务逻辑)                   │
│     └── tasks/                                    │
│         ├── index.md                              │
│         └── YYYY/      (按年归档)                   │
└──────────────────────────────────────────────────┘
```

**核心原则**:
- 与现有 `memory.json` 完全独立，通过 `config.yaml` 独立开关
- Markdown 文件存储，人类可读可编辑
- 实体关联通过 markdown 链接表达，无需单独关系存储
- 仅注入摘要索引进 system prompt，详情按需通过工具读取
- 一个 skill (`structured-memory`) 定义判断逻辑和工作流

## File Structure

```
structured_memory/
├── README.md                    # 整体说明（agent 自动维护）

├── facts/                       # 事实记忆
│   ├── index.md                 # 事实总索引
│   │   # 每条: - [实体名](相对路径.md) — 简介
│   │   # 按 schema 和 business 分节
│   │
│   ├── schema/                  # 数仓架构
│   │   ├── index.md             # 库表索引
│   │   │   # 每条: - [db.table](tables/xxx.md) — 表描述
│   │   ├── tables/              # 表详细定义
│   │   │   ├── ods_order.md
│   │   │   └── ...
│   │   └── fields/              # 跨表通用字段/指标
│   │       └── common_metrics.md
│   │
│   └── business/                # 业务逻辑
│       ├── index.md             # 业务概念索引
│       │   # 每条: - [概念名](xxx.md) — 一句话说明
│       └── revenue_def.md       # 收入口径定义

└── tasks/                       # 任务记忆
    ├── index.md                 # 任务索引（按时间倒序）
    │   # 每条: - [YYYY-MM-DD 任务标题](YYYY/task.md) — 摘要
    └── 2026/
        └── 2026-Q1-sales-analysis.md
```

**链接示例** (`facts/schema/tables/ods_order.md`):
```markdown
# ods_order (订单明细表)

## 基本信息
- 库: ods
- 表: order
- 分层: ODS
- 更新频率: 日

## 字段
| 字段 | 类型 | 说明 |
|------|------|------|
| order_id | string | 订单ID |
| ...

## 关联
- 业务定义: [收入口径](../business/revenue_def.md)
- 下游表: [dwd_trade](dwd_trade.md)
- 相关任务: [2026-Q1 销售分析](../../../tasks/2026/2026-Q1-sales-analysis.md)
```

## Built-in Tools (6 个)

| 工具 | 签名 | 用途 |
|------|------|------|
| `search_structured_memory` | `(query: str, category: "facts"\|"tasks"\|"all" = "all") -> str` | 全文搜索记忆内容，返回匹配文件路径和摘要 |
| `get_memory_entity` | `(path: str) -> str` | 读取指定实体文件完整内容，path 为相对路径 |
| `list_memory_entities` | `(path: str = "", depth: int = 2) -> str` | 浏览目录结构，默认展示 2 层 |
| `write_memory_entity` | `(path: str, content: str) -> str` | 创建或覆写实体文件，原子写入 |
| `delete_memory_entity` | `(path: str) -> str` | 删除实体文件 |
| `update_memory_index` | `(index_path: str, action: "add"\|"remove"\|"update", entry: str, target: str = "") -> str` | 安全更新索引文件 |

**路径约定**: 所有工具接受相对路径（如 `facts/schema/tables/ods_order.md`），底层自动映射到 `{base_dir}/structured_memory/`。agent 不需要知道绝对路径。

**与沙箱工具的关系**: 6 个工具操作的是结构化记忆存储目录（`{base_dir}/structured_memory/`），与沙箱 workspace（`/mnt/user-data/...`）是独立的存储空间。`write_memory_entity` 使用原子写入（临时文件 + rename），安全可靠。`update_memory_index` 保证索引格式一致性。

## Config

```yaml
structured_memory:
  enabled: true              # 总开关
  storage_path: ""           # 留空默认 {base_dir}/structured_memory/
  injection_enabled: true    # 是否注入摘要到 system prompt
  max_index_tokens: 1500     # 注入索引的 token 预算
```

新增 `StructuredMemoryConfig` Pydantic 模型（`packages/harness/deerflow/config/structured_memory_config.py`），注册到 `AppConfig`。

## Prompt Injection

在 `SYSTEM_PROMPT_TEMPLATE` 中 `{memory_context}` 之后新增 `{structured_memory_context}`。

注入内容（token 预算内）:
1. `facts/index.md` 摘要 — 列出事实知识分类和关键实体
2. `tasks/index.md` 摘要 — 最近 N 条任务记录

告示 agent 可通过 `get_memory_entity` / `search_structured_memory` 按需获取详情。

## Skill: structured-memory

`skills/public/structured-memory/SKILL.md`:

```yaml
---
name: structured-memory
description: 企业结构化记忆的识别、分类、更新规范——含事实记忆（数仓架构、业务逻辑）和任务记忆的判断逻辑与工作流
license: MIT
---
```

Skill 定义:
- **何时需要更新记忆** — 用户提供新的企业数据信息、完成任务后、纠正历史记忆
- **事实记忆判断逻辑** — 如何判断信息属于 schema 还是 business，字段定义模板
- **任务记忆判断逻辑** — 何时将任务抽象为记忆，总结模板
- **索引更新规范** — index.md 的格式约定（markdown 无序列表 + 链接 + 简介）
- **关联建立规范** — 如何在实体文件中添加反向链接

## Implementation Steps

1. **配置层** — `structured_memory_config.py` + 注册到 `app_config.py`
2. **存储层** — `packages/harness/deerflow/structured_memory/` 模块（storage, 路径管理, 索引解析）
3. **工具层** — 4 个 tools in `tools/builtins/`
4. **注入层** — `prompt.py` 新增 `{structured_memory_context}`
5. **Skill** — `skills/public/structured-memory/SKILL.md`
6. **测试** — `tests/test_structured_memory.py`

## Verification

1. 启动 agent，确认 `config.yaml` 开关可控制注入
2. agent 对话中能通过 4 个工具读写结构化记忆
3. agent 能根据 skill 指导，自主判断何时创建/更新记忆
4. 索引文件格式符合约定，跨文件链接有效
5. 现有 personal memory 系统不受影响
