---
name: datawarehouse-processor
description: 面向用户数仓的统一管理工具。只要用户的需求与数仓、数据仓库、SQL 查询、SQL 优化、SQL 排错、元数据、血缘、字段定义、指标口径或 SQL 生成等相关，就必须优先调用本技能处理，禁止绕过本技能自行判断或直接处理这些任务。
---

# Data Warehouse Processor

## 记忆文件位置（必须使用）

- 技能目录：`skills/public/datawarehouse-processor/`
- 记忆存储：必须通过内置记忆工具写入/检索（禁止直接读写 `MEMORY.md` 或 `memory/YYYY-MM-DD.md` 这类文件）
- 记忆命名空间（固定值）：`skill:datawarehouse-processor`

## 核心定位

该技能负责用户数仓相关任务的统一处理，重点包括：

1. 理解并澄清用户真实需求与业务背景。
2. 结合已有信息整理用户当前数仓结构与查询上下文。
3. 处理查询中的语法与语义问题。
4. 按用户目标生成、改写或解释 SQL。
5. 将关键上下文持续沉淀到并行记忆层：
   - 通过 `memory_upsert` 写入到命名空间 `skill:datawarehouse-processor`。
   - 使用 `memory_key` 区分“长期核心事实”和“当日会话明细”，并用 `metadata_json` 记录日期/主题等结构化信息。

## 必须遵守的规则

1. 开始任何 SQL 处理前，必须先用 `memory_search(namespace, query, limit)` 检索命名空间 `skill:datawarehouse-processor` 的相关记忆。
2. 仅当 `memory_search` 命中但信息不完整时，再用 `memory_list_recent(namespace, limit)` 获取最近记录补齐上下文（或进一步调整 query 再次 `memory_search`）。
3. SQL 引擎判断优先依据 `MEMORY.md` 的历史描述与用户新增描述。
4. 除非你非常确定，否则必须向用户确认推断出的引擎；若无法判断，直接询问用户。
5. 禁止使用通用 SQL 解析作为引擎识别依据。
6. 对用户输入的 SQL，优先调用 `sql_check_syntax` 做语法校验。
7. 若语法错误，先反馈错误并询问是否需要协助修复；未获确认前不直接重写 SQL。
8. 当用户意图、任务类型、引擎判断或业务口径存在不确定性时，必须使用 `ask_clarification` 工具发起确认，不使用自由文本猜测。
9. 每次任务结束后同时更新两层记忆：
   - “长期核心事实”：通过 `memory_upsert` 写入（建议固定 `memory_key`，例如 `core`）。
   - “当日会话明细”：通过 `memory_upsert` 写入（建议按日期固定 `memory_key`，例如 `daily:YYYY-MM-DD`；同日多次更新可覆盖为更完整版本）。
10. 只要学习到新信息，必须记录。新信息来源包括但限于：
   - 主动查询遇到错误并定位原因。
   - 反复调优后得到可复用结论。
   - 用户主动提供的业务细节、口径和约束。

## When to write memory

- Decisions, preferences, and durable facts go to `memory_upsert(namespace="skill:datawarehouse-processor", memory_key="core", ...)`.
- Day-to-day notes and running context go to `memory_upsert(namespace="skill:datawarehouse-processor", memory_key="daily:YYYY-MM-DD", ...)`.
- If someone says "remember this," write it down (do not keep it in RAM).
- This area is still evolving. It helps to remind the model to store memories; it will know what to do.

## 执行流程

### Step 0 - 读取记忆

开始处理前必须先用 `memory_search` 检索命名空间 `skill:datawarehouse-processor`，提取：
- 用户已确认的数据引擎或数据库方言。
- 表结构、字段口径、指标定义、历史约束。
- 业务特例说明（如多 UNION 的业务原因）。

当 `memory_search` 命中但信息不足以支撑当前任务时，再用 `memory_list_recent` 拉取最近记录补全细节。

#### 读取方式（强制）

1. 调用 `memory_search(namespace="skill:datawarehouse-processor", query="<当前任务关键词>", limit=5)`。
2. 从返回的 `hits` 中提取与引擎、表、字段、指标、血缘、业务口径相关的记忆项内容与元数据。
3. 若命中信息完整，直接进入后续步骤。
4. 若命中但信息不完整：
   - 调用 `memory_list_recent(namespace="skill:datawarehouse-processor", limit=20)` 获取最近记忆；
   - 或调整 query 后再次 `memory_search`（例如加上表名/指标名/系统名/引擎名）。

### Step 1 - 识别任务类型

将用户请求归类为：
- SQL 问题排查（报错、语法、执行异常）。
- SQL 生成或改写（按业务需求写 SQL）。
- 元数据/血缘/定义查询（解释表、字段、指标来源）。
- 性能优化（慢查询、改写建议）。

若类型不清晰，先使用 `ask_clarification` 工具澄清后再继续。

### Step 2 - 引擎推断与确认（强制）

按以下顺序执行：
1. 从命名空间 `skill:datawarehouse-processor` 的记忆检索用户历史引擎描述（优先从 `memory_search` 命中项中提取）。
2. 结合本轮用户描述做引擎推断。
3. 输出推断结论与依据。
4. 执行确认：
   - 非“非常确定”场景：必须通过 `ask_clarification` 工具确认“是否为该引擎”。
   - 完全无法判断：直接请用户指定引擎。

禁止使用通用 SQL 解析去“自动判定引擎”。

#### ask_clarification 使用规范

- 触发条件：意图不清、任务类型冲突、口径不完整、引擎不确定、用户目标存在多种解释。
- 输出要求：问题要结构化、可选择、可执行，避免开放式泛问。
- 执行时机：在任何可能导致错误 SQL 或错误业务解释的步骤之前。
- 处理原则：收到用户确认后再继续执行 SQL 生成、改写、优化或元数据分析。

### Step 3 - SQL 语法校验优先

当用户输入包含 SQL 时，先调用：
- `sql_check_syntax(sql, source_dialect)`

处理分支：
- **校验失败**：返回错误位置和原因，询问“是否要我直接修复并给出可执行版本”。
- **校验通过**：进入 Step 4 执行用户目标任务。

### Step 4 - 按目标执行任务

#### A. 排错与修复
- 在语法结果基础上定位问题。
- 若用户确认需要修复，给出修复 SQL 与修改说明。

#### B. SQL 生成
- 明确筛选范围、指标口径、分组维度、时间边界。
- 若信息不足，先提缺失项，不生成猜测 SQL。

#### C. 元数据与血缘
- 使用可用工具提取表、字段、CTE、依赖关系。
- 对关键语句补充简明业务说明，避免仅给技术字段列表。

#### D. 性能优化
- 先确认优化目标（时延、资源、可读性、稳定性）。
- 提供可落地改写与影响说明，不做无依据承诺。

### Step 5 - 记忆沉淀与更新

将本轮新增信息同步写入两层记忆：
- “长期核心事实”：用 `memory_upsert(namespace="skill:datawarehouse-processor", content=..., memory_key="core", source_skill="datawarehouse-processor", metadata_json=...)` 写入（必写，写核心结论）。
- “当日会话明细”：用 `memory_upsert(namespace="skill:datawarehouse-processor", content=..., memory_key="daily:YYYY-MM-DD", source_skill="datawarehouse-processor", metadata_json=...)` 写入（必写，写完整细节）。

写入关系约束：
- `memory_key="daily:YYYY-MM-DD"` 的内容必须包含比 `memory_key="core"` 更完整的细节。
- 禁止出现“已写入核心记忆，因此不写当日明细”的情况。

两层内容至少包含：
- 用户输入摘要（保留关键原话）。
- 明确的业务背景与约束说明。
- 引擎推断与最终确认结果。
- 关键口径/表结构/字段定义更新。
- 输出结果摘要与后续待确认项。

#### 新增知识强制记录规则

出现以下任一事件，必须写入记忆：
- 查询或执行过程中出现错误，并完成定位或修复。
- 多轮调优后形成稳定方案或约束。
- 用户提供新的业务背景、口径定义、数据分表原因、平台差异说明。
- 新确认的引擎、方言、表关系、字段语义、指标计算规则。

## MEMORY.md 写入规范（全局总结）

每次更新“长期核心事实”建议使用以下结构组织 `content`（写入 `memory_key="core"`）：

```markdown
## YYYY-MM-DD - <topic>
- Cross-Session Facts:
- Stable Business Rules:
- Confirmed Engine/Platform:
- Key Tables and Metrics:
- Reusable Decisions:
- Follow-up Risks:
```

## 当日明细写入规范（当日会话明细）

当日明细写入到 `memory_key="daily:YYYY-MM-DD"`（例如 `daily:2026-03-31`）。

```markdown
## HH:MM - <task tag>
- User Intent:
- Engine Guess:
- Engine Confirmed:
- Business Context:
- SQL/Metadata Notes:
- Decisions:
- Open Questions:
```

### 业务背景记录示例

- “这段代码采用多 UNION 拼接，是因为同类指标分布在不同业务域/平台的不同物理表中。”
- “该字段命名看似重复，但在 A 系统表示订单创建时间，在 B 系统表示支付落单时间。”
- “用户要求按自然周统计，但财务口径以 T+1 入账为准。”

## 默认回复策略

1. 先说明当前使用的引擎推断和确认状态。
2. 对 SQL 请求先给语法校验结论。
3. 再给后续处理结果或所需补充信息。
4. 明确标注哪些结论来自 `MEMORY.md`、哪些来自 `memory/YYYY-MM-DD.md`、哪些来自本轮新增输入。

## 输出模板

### 语法异常场景

```markdown
检测到 SQL 语法问题：
- Engine: <guess/confirmed>
- Error: <message>
- Position: <line/column if available>

是否需要我基于该引擎直接修复并返回可执行 SQL？
```

### 继续执行场景

```markdown
语法检查通过，继续执行你的目标任务。
- Engine: <guess/confirmed>
- Goal: <debug/generate/metadata/optimize>
- Next Step: <what will be done next>
```

## 质量要求

- 不猜测未提供的业务口径。
- 不跳过引擎确认直接做方言敏感改写。
- 不在未校验语法时直接进入复杂优化。
- 不遗漏 `MEMORY.md` 与 `memory/YYYY-MM-DD.md` 的更新。
- `memory/YYYY-MM-DD.md` 必须保留当日更多细节，不能只复制 `MEMORY.md` 的摘要。
