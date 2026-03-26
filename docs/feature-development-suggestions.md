# 新功能开发建议（参考）

> 以下为规划与策略参考，实际开发须以用户需求与当前仓库代码为准。

## 六、对应你产品功能的开发指南

下面针对你规划的7个功能，逐一分析复用程度和开发策略。

---

### ✅ 功能1：SQL文件解析 → 数仓血缘/字段定义

| 评估项 | 内容 |
|---|---|
| **复用程度** | 中 |
| **可复用模块** | 文件上传系统（`routers/uploads.py`）、`read_file` 工具、Memory 系统（存储血缘图）、Skill 系统（SQL解析 Skill） |
| **需新开发** | SQL AST 解析器（推荐 `sqlglot`/`sqlfluff`）、数仓元数据数据库（Postgres/SQLite 存储表/列/血缘关系）、新增 API Router（`/api/warehouse/lineage`等） |

**开发策略：**
- 在 `skills/custom/` 下新建 `sql-parsing` Skill，包含解析 SQL 的提示词工程
- 新增 Tool（注册到 `config.yaml` tools 列表）：`parse_sql_lineage`，调用 sqlglot 解析 DDL/DML
- 新增 FastAPI Router 在 Gateway 层，提供元数据持久化 CRUD API

**配置参考（新增 Tool 的方式）：** [config.example.yaml:163-226]
---

### ✅ 功能2：UI 血缘关系网络图

| 评估项 | 内容 |
|---|---|
| **复用程度** | 低（UI 从头开发，但框架可复用） |
| **可复用模块** | Next.js 框架、shadcn/ui 组件库、`workspace/` 页面路由结构、`core/api/` HTTP 客户端 |
| **需新开发** | 图可视化页面（推荐 `React Flow` 或 `@antv/g6`）、新页面路由 `/workspace/warehouse`、点击节点展示字段信息的 Drawer 组件 |

**开发策略：**
- 在 `frontend/src/app/workspace/` 下新增 `warehouse/` 路由
- 在 `frontend/src/core/` 下新增 `warehouse/` 模块（API Client + Hooks）
- 在 `frontend/src/components/workspace/` 下新增 `lineage-graph.tsx` 组件

---

### ✅ 功能3：根据输入生成 SQL

| 评估项 | 内容 |
|---|---|
| **复用程度** | 高 |
| **可复用模块** | 完整 Lead Agent 系统、Memory 系统（存储库表结构为 facts）、Skill 系统 |
| **需新开发** | `sql-generation` Skill（含数仓 Schema 上下文注入的提示词）、可选：专属 Custom Agent（带 SQL 工具组限制） |

**开发策略：**
- 创建 Custom Agent（通过 `POST /api/agents`），指定 `tool_groups: ["file:read", "bash"]`，注入数仓 Schema 作为 SOUL.md 内容
- 在 Memory 中存储用户常用表结构作为 facts，自动注入 System Prompt： [updater.py:40-56]

---

### ⚠️ 功能4：Browser 自动化（模拟 IDE 输入 SQL + 下载数据）

| 评估项 | 内容 |
|---|---|
| **复用程度** | 中（架构层可复用，具体能力需新增） |
| **可复用模块** | MCP 协议层（最佳接入点）、Sandbox bash 工具（备选方案） |
| **需新开发** | 基于 Playwright 的 MCP Server（`@playwright/mcp` 或自研）、Browser 会话管理、截图确认机制 |

**最佳开发策略：** 通过 MCP Server 接入，无需改动核心 Agent 代码： [tools.py:14-66]

在 `extensions_config.json` 中注册你的 Browser MCP Server：

```json
{
  "mcpServers": {
    "browser": {
      "type": "stdio",
      "command": "npx",
      "args": ["@playwright/mcp"],
      "enabled": true
    }
  }
}
```

工具自动注册到 Agent，无需修改任何 Agent 代码。

---

### ✅ 功能5：自主拆解任务循环分析（数据下跌原因分析等）

| 评估项 | 内容 |
|---|---|
| **复用程度** | 高（这是 DeerFlow 的核心设计目标） |
| **可复用模块** | Subagent 系统（并发执行多维度查询）、Loop Detection（防止死循环）、Plan Mode（Todo 任务追踪）、LoopDetectionMiddleware |
| **需新开发** | 数据分析专属 Subagent 配置（`builtins/data_analysis_agent.py`）、SQL 执行 Tool（调用数据库并返回结果）、终止条件 Tool |

**开发策略：**

1. 新增 `sql_execute` Tool（注册到 `config.yaml`），通过数据库连接执行 SQL 并返回 DataFrame 摘要
2. 新增 `data-analyst` Subagent（在 `subagents/builtins/` 下），专注数据查询与假设验证
3. 开启 `subagent_enabled=True` + `is_plan_mode=True`，Lead Agent 会自动使用 TodoList 追踪分析步骤

**复用 Subagent 并发机制：** [prompt.py:17-93]

**复用循环检测 Middleware：** [agent.py:249-259]

---

### ✅ 功能6：自定义执行指导 + 历史经验自主学习

| 评估项 | 内容 |
|---|---|
| **复用程度** | 高 |
| **可复用模块** | **Skills 系统**（自定义执行指导）、**Memory 系统**（历史经验学习）、**Custom Agent SOUL.md**（固定行为指南） |
| **需新开发** | 业务特定 Skill 文件（`skills/custom/data-analysis/SKILL.md`）、可能需要扩展 Memory 的 facts schema |

**执行指导 → 写入 Skills（Agent 按需加载）：** [loader.py:22-74]

**历史经验学习 → Memory 系统自动提取：** [updater.py:244-308]

Memory 支持 per-agent 独立存储（不同任务类型的经验互不干扰）： [updater.py:19-37]

---

### ✅ 功能7：自定义 Skill 或 Tool

| 评估项 | 内容 |
|---|---|
| **复用程度** | 极高（开箱即用） |
| **可复用模块** | 完整 Skill/Tool 注册机制 |
| **需新开发** | 仅需编写业务逻辑本身 |

**自定义 Tool**（Python 函数 + `@tool`）

