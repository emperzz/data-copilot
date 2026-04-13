# StructuredMemory 通用记忆管理（并行双记忆）开发方案（MVP）

## 1. 目标与约束

### 1.1 目标

- 在现有项目上构建**StructuredMemory 通用记忆管理能力**，服务于 agent/skill 的知识沉淀与检索。
- StructuredMemory 覆盖：业务特征、变化历史、库表逻辑、规则约束、排障经验等，不局限于用户偏好。
- 与现有会话记忆并行共存，尽可能复用现有流程能力（中间件、队列、配置、工具注册、注入策略）。

### 1.2 关键约束

- StructuredMemory 不落 `memory.json`，采用 Chroma 持久化。
- **阶段性策略（已调整）**：当前迭代**不实现**用户审批/确认闭环；**是否写入、写入哪一层（raw/distilled/core）、类型与正文**由 **agent 依据系统提示与 Skill 规范自主决定**，通过**显式工具调用**落库。`ask_clarification` 式「先提议—再人工确认—再提交」整段能力**待主流程（写入 + 检索 + 业务验证）跑通后再开发**（对应原 MVP-2）。
- 与会话记忆的「回合结束后异步自动提炼」区分：StructuredMemory **不走** `MemoryMiddleware` 静默队列；未调用写入工具则不应产生 StructuredMemory 记录。
- 方案基于当前仓库事实，优先最小化 MVP，后续可按效果重构。

### 1.3 当前代码基线（用于设计决策）

- 已有会话记忆主链路：`MemoryMiddleware.after_agent()` -> `MemoryUpdateQueue.add()` -> `MemoryUpdater.update_memory()` -> `MemoryStorage.save()`。
- `MemoryStorage` 当前抽象的数据契约是 `dict[str, Any]` 的单体 memory payload（对应 `memory.json`）。
- 已有 Chroma 分层仓储基础：`deerflow.memory.models`、`deerflow.memory.repository.StructuredMemoryRepository`，支持 `raw/distilled/core` 三层的 create/get。
- 尚无面向 agent 的 **StructuredMemory 显式写入工具链**（规范化、按 tier 写入、来源字段）与 **统一查询工具**；**用户确认后提交**的编排（确认状态机、多轮修订）刻意后移，不在当前迭代范围。

---

## 2. 并行双记忆总体设计

### 2.1 两套记忆的职责拆分

- **会话记忆（现有）**
  - 目标：提升对当前用户协作习惯与近期上下文的连续性。
  - 特征：会话后异步自动提炼，注入系统提示词。
  - 存储：`MemoryStorage`（默认文件）。

- **StructuredMemory（新增）**
  - 目标：沉淀企业业务知识与结构化事实，服务跨会话、跨任务复用。
  - 特征（当前阶段）：**由 agent 在 prompt/skill 约束下决定写入范围、记忆类型与内容**，经工具显式写入；**持久化字段仍保留来源信息**，便于后续审计与追溯。人工确认入库为**后续阶段**，不阻塞当前主流程。
  - 存储：`StructuredMemoryRepository`（Chroma）。

### 2.2 关于 `MemoryStorage` 是否可复用

- **可复用（建议复用）**
  - Provider 抽象思想（`MemoryStorage` + `get_memory_storage()` 的动态装配模式）。
  - 单例与线程锁初始化模式。
  - 基础配置加载方式（`memory_config` 风格）。

- **不建议直接复用为 StructuredMemory 主接口**
  - `MemoryStorage` 当前契约是“加载/保存整个 memory 字典”，与 StructuredMemory“多条记录、分层、可追溯来源、按记录 CRUD”的语义不匹配（**是否**经人工确认再入库为产品阶段策略，不改变上述接口差异）。
  - StructuredMemory 需要 `create/query/list/link/update_status` 等面向记录的 API，不适合 `load/save(dict)`。（当前阶段以 `create` + 后续 `query` 为主；`update_status` 等可随治理阶段扩展。）

- **建议操作**
  - 保留 `MemoryStorage` 处理会话记忆。
  - 新增 `StructuredMemoryStore` 抽象（或 `StructuredMemoryRepository` 门面），底层首个实现接 `StructuredMemoryRepository`。
  - 两套记忆共用中间件装配、配置加载、工具注册模式；**StructuredMemory 的确认机制暂缓**，与 `ClarificationMiddleware` 的耦合留待原 MVP-2 启用时再接。

---

## 3. 记忆管理流程图（函数级，当前阶段）

**当前阶段（无审批）**：写入由模型在 prompt/skill 约束下**主动调用工具**完成；检索与复用见 MVP-3。

```mermaid
flowchart TD
    A["对话上下文 + Skill 策略"] --> B["LeadAgent 执行"]
    B --> C{"是否调用 StructuredMemory 写入工具"}
    C -- 否 --> Z["结束 不写 StructuredMemory"]
    C -- 是 --> D["StructuredMemoryWriteService 规范化草案"]
    D --> E{"目标 tier / 类型由工具参数或策略决定"}
    E --> F1["create_raw_memory"]
    E --> F2["create_distilled_memory"]
    E --> F3["create_core_memory"]
    F1 --> G["返回 memory_id 与摘要"]
    F2 --> G
    F3 --> G
    G --> H["StructuredMemoryTool query 检索复用"]
    H --> I["后续轮次或任务复用"]
```

**后续阶段（验证通过后，原确认闭环）**：在 `G` 之后可插入 `ask_clarification`、确认中间件与 `StructuredMemoryCommitService`，将「直接写 tier」改为「确认后再 promote」；详略同原 MVP-2 设计，实施顺序见第 7 节。

---

## 4. 流程节点归属：原有 vs 新增 + MVP映射

| 流程节点 | 类型 | 说明 | 落地MVP |
|---|---|---|---|
| `LeadAgent` 执行主链路 | 原有 | 现有 agent 装配与运行 | 已有 |
| `ask_clarification` + `ClarificationMiddleware` | 原有 | 通用澄清能力仍在；**当前不接入** StructuredMemory 审批链 | 已有（未接 SM） |
| `StructuredMemoryRepository.create_*` | 原有（基础） | Chroma 各层写入 | 已有 |
| `StructuredMemoryWriteTool`（或等价内置工具） | 新增 | agent 显式写入；**tier / raw_kind / 内容 / 标签**由调用参数体现「自主决策」结果 | **MVP-1（当前优先）** |
| `StructuredMemoryWriteService.build_record(...)` | 新增 | 与原 `DraftService` 职责类似：规范化、来源、幂等、长度与类型校验 | **MVP-1** |
| `StructuredMemoryTool.query()` | 新增 | agent/skill 统一检索 | **MVP-3（当前优先，与写入并列验证）** |
| `StructuredMemoryConfirmationMiddleware` / `CommitService` / `mark_rejected` 等 | 新增 | 用户确认后再 promote | **暂缓（原 MVP-2，主流程验证后）** |
| `StructuredMemoryIndexService.update_indexes()` | 新增 | 检索优化、标签索引 | MVP-3/4 |
| 记忆生命周期治理（归档/冲突合并） | 新增 | 后续增强 | MVP-4 |

---

## 5. MVP 拆分与实施细节

## MVP-0（当前进度基线）

### 需实现/已有状态

- 已具备：
  - 会话记忆完整链路（自动提炼 + 注入）。
  - Chroma 分层 repository 基础数据结构与 create/get。
  - Clarification 通用能力（当前 StructuredMemory **不依赖**其做审批）。
- 未具备（当前迭代要补）：
  - agent 显式 **写入** StructuredMemory 的工具与服务（含规范化与来源字段）。
  - 面向 agent/skill 的 **统一查询**工具。
- 刻意延后：
  - StructuredMemory **用户确认状态机**与「确认后再 promote」编排（原 MVP-2）。

### 设计原因

- 先承认已有基础，再做最小补齐，避免推倒现有 memory 体系。

### 项目影响

- 仅形成基线，不改行为。

### upstream 合并冲突简案

- 当前阶段无新增代码冲突，只需持续关注 `agents/memory/*` 与 `memory/*` 目录上游变更。

### 当前进度

- 完成（基线确认）。

### 新增函数与配置清单（本阶段定义，不落代码）

- `StructuredMemoryRecordStatus`（枚举，新增）
  - 作用：统一记录生命周期状态（如 `active` / `superseded` / `archived`）；**后续**若启用确认流，可再并入 `pending_confirmation` / `rejected` 等，避免字符串分叉。
  - 设计理由：先定义跨模块契约；当前阶段以「已写入后的治理态」为主，确认态为扩展预留。
- `StructuredMemorySourceRef`（结构体，新增）
  - 作用：统一 source 信息字段（thread/file/url/image/tool_call），供 raw/distilled/core 共用。
  - 设计理由：StructuredMemory 强调可追溯，先固定来源结构有利于查询和审计。
- `structured_memory.enabled`（配置，新增）
  - 作用：StructuredMemory 总开关，与现有 `memory.enabled` 解耦。
  - 设计理由：会话记忆与 StructuredMemory 要可独立启停，避免互相影响。
- `structured_memory.store`（配置，新增）
  - 作用：声明 StructuredMemory 后端类型，首版固定 `chroma`。
  - 设计理由：为后续引入 PGVector/ES 预留扩展点。

---

## MVP-1：agent 显式写入（当前优先）

### 需实现功能细节

- 新增 **`StructuredMemoryWriteTool`**（名称可微调，职责不变）：agent/skill 在需要沉淀知识时**主动调用**。
  - 输入（示例维度，以实现为准）：`tier`（`raw` / `distilled` / `core`）、`content`、`tags`、`raw_kind`（写 raw 时必填）、**来源字段**（与现有 `RawMemoryRecord` 校验规则一致，或对齐未来的 `StructuredMemorySourceRef`）、若写 distilled/core 则 **`raw_memory_ids` / `distilled_memory_ids`** 等关联字段。
  - 输出：`memory_id`、写入 tier、摘要；错误时返回可行动说明（缺字段、超长、非法 tier 组合等）。
  - **范围、类型与内容**不在工具内用硬编码策略代替模型判断，而由 **系统提示 + Skill** 约束「何时写、写什么层、写什么主题」；工具只做**校验与持久化**。
- 新增 **`StructuredMemoryWriteService`**（可与原 `DraftService` 合并命名，但职责以写入为主）：
  - 规范化正文与标签、补全默认来源（如当前 `thread_id`）、幂等策略（可选，按 `content + source + tier`）、长度上限。
  - 调用已有 `StructuredMemoryRepository.create_raw_memory` / `create_distilled_memory` / `create_core_memory`。
- 新增配置段（建议）：
  - `structured_memory.enabled`
  - `structured_memory.store=chroma`
  - `structured_memory.write.max_content_length`、`structured_memory.write.allowed_tiers`、`structured_memory.write.allowed_raw_kinds`（运维侧约束；**不等同**于代替模型决策）
- 新增最小测试：
  - write tool/service 单测（各 tier 成功路径、校验失败路径）。
  - 与 Chroma 集成的冒烟测试（若已有 harness）。

### 为什么这么设计

- 主流程先闭环「能写、能查、能验证业务价值」；审批流后置，避免在未验证检索与数据质量前堆交互状态机。
- 写入与会话异步记忆解耦：只有调用工具才落 StructuredMemory，职责清晰。

### 对当前项目影响

- 增加写入工具 + 服务；不改变 `MemoryMiddleware` 会话记忆链路。
- StructuredMemory 与 `memory.json` 仍完全隔离。
- **风险说明**：无人工确认时，模型可能过写或误写，需靠 prompt/skill、工具组权限与配置上限缓解；验证通过后再上 MVP-2。

### upstream 合并冲突简案

- 新代码集中在 `deerflow/memory/` 与 `tools/builtins/` 新文件，减少改动热点。
- 如上游更新 `tools` 注册逻辑，优先保持新增工具在扩展注册点接入，避免改核心分发代码。

### 当前进度

- 未开始（**当前建议第一优先级**）。

### 新增函数与配置清单（MVP-1 实现）

- `StructuredMemoryWriteTool.write(...)`（新增）
  - 作用：agent/skill 统一写入入口；参数表达「模型决定的 tier/类型/内容」。
  - 设计理由：权限、配额、审计日志可集中在工具层。
- `StructuredMemoryWriteService.normalize_and_write(...)`（新增）
  - 作用：校验 + 规范化 + 调用 repository。
  - 设计理由：避免每个 tier 在 tool 内复制分支。
- `StructuredMemoryWriteService.compute_idempotency_key(...)`（新增，可选）
  - 作用：降噪重复写入。
  - 设计理由：自主写入阶段模型仍可能重复调用工具。
- `structured_memory.write.max_content_length`（配置，新增）
  - 作用：限制单条长度。
  - 设计理由：性能与成本护栏。
- `structured_memory.write.allowed_tiers` / `allowed_raw_kinds`（配置，新增）
  - 作用：部署级白名单。
  - 设计理由：分环境收紧能力面。
- **以下条目暂缓或改为后续确认流专用**：`require_confirmation`、`get_draft(proposal_id)` 仅在与 MVP-2 一并实现时再有必要。

---

## MVP-2：确认后提交闭环（暂缓 — 主流程验证后再开发）

> **说明**：本节保留原设计作为后续增量。当前路线为 **MVP-1 写入 + MVP-3 查询** 先验收；待效果与数据质量评估通过后，再实现人工确认、`CommitService` 与多轮修订，将「直接写 tier」迁移或并存为「先草案再确认后 promote」。

### 需实现功能细节

- 新增 `StructuredMemoryConfirmationMiddleware`：
  - 监听包含 `proposal_id` 的确认回复。
  - 将用户回复映射为 `confirm/revise/reject`。
- 新增 `StructuredMemoryCommitService`：
  - `confirm`：raw -> distilled -> core（调用现有 repository create 接口）。
  - `revise`：更新草案内容，重新发起 `ask_clarification`。
  - `reject`：标记草案拒绝状态，不进入 distilled/core。
- 新增状态模型（建议）
  - `draft / pending_confirmation / confirmed / rejected / committed / revised`
  - 记录 `confirmed_by`、`confirmed_at`、`revision_count`。
- 新增测试
  - 中间件确认流转测试。
  - 多轮 revise 后再 confirm 测试。
  - reject 不落 core 的保护测试。

### 为什么这么设计（在启用 MVP-2 时）

- 在需要强合规/强 curated 知识库时，用人工确认降低误写入。
- 将「确认逻辑」与「提交逻辑」分离，便于与 `ask_clarification` 复用。

### 对当前项目影响（在启用 MVP-2 时）

- Agent 流程中新增 StructuredMemory 确认分支；提示词需区分「仅工具直写」与「先提议后确认」两种模式（可通过配置切换）。

### upstream 合并冲突简案

- 中间件注入尽量通过 `custom_middlewares` 或独立工厂函数接入，减少直接修改 `make_lead_agent` 关键路径。
- 若上游调整 Clarification 机制，保持 `StructuredMemoryConfirmationMiddleware` 仅依赖标准 `ToolMessage`/state 字段，降低耦合。

### 当前进度

- **暂缓**；优先级让位于 MVP-1 + MVP-3。

### 新增函数与配置清单（MVP-2 实现）

- `StructuredMemoryConfirmationMiddleware.after_agent(state, runtime)`（新增）
  - 作用：识别确认回复并驱动 `confirm/revise/reject` 状态流转。
  - 设计理由：复用现有 middleware 链，不侵入主 agent 节点实现。
- `StructuredMemoryConfirmationMiddleware._parse_confirmation_intent(message)`（新增）
  - 作用：把自然语言回复映射成确认意图与修订内容。
  - 设计理由：确认判定集中治理，便于后续加入更严格解析策略。
- `StructuredMemoryCommitService.commit(proposal_id, confirmer)`（新增）
  - 作用：将已确认草案提交到 distilled/core，并写提交审计字段。
  - 设计理由：把提交动作收敛到单一事务入口，降低跨层调用复杂度。
- `StructuredMemoryCommitService.revise(proposal_id, revision_note)`（新增）
  - 作用：更新草案文本与版本号，返回下一轮确认提示。
  - 设计理由：支持“反复确认”必须具备修订闭环，而不是确认失败即终止。
- `StructuredMemoryCommitService.reject(proposal_id, reason)`（新增）
  - 作用：标记拒绝并保留原因，用于后续分析错误提议模式。
  - 设计理由：拒绝记录是治理数据，不应直接删除。
- `StructuredMemoryCommitService.promote_to_distilled(raw_id, links)`（新增）
  - 作用：从 raw 提炼为 distilled，并建立来源引用关系。
  - 设计理由：显式分层可避免后续把业务事实直接混入 core。
- `StructuredMemoryCommitService.promote_to_core(distilled_ids)`（新增）
  - 作用：合并高价值 distilled 进入 core，形成可稳定复用的事实层。
  - 设计理由：core 应保持高质量、低噪声，必须通过确认后路径进入。
- `structured_memory.confirmation.max_rounds`（配置，新增）
  - 作用：限制最多修订轮次，防止无限循环确认。
  - 设计理由：保护交互体验和模型成本。
- `structured_memory.confirmation.timeout_minutes`（配置，新增）
  - 作用：草案待确认超时控制，过期转 `expired` 或 `rejected`。
  - 设计理由：企业场景里长时间未确认的草案通常价值衰减。
- `structured_memory.confirmation.require_proposal_id`（配置，新增）
  - 作用：确认回复必须携带 `proposal_id`，否则不提交。
  - 设计理由：避免多草案并行时的误提交。

---

## MVP-3：统一查询能力（agent/skill 可直接使用）

### 需实现功能细节

- 新增 `structured_memory_query` 工具（或 `StructuredMemoryTool.query`）：
  - 输入：`query_text/tier_filter/tags/user/agent/top_k/time_range`。
  - 输出：可解释结果（内容、置信度、来源链路、更新时间）。
- 新增 `StructuredMemorySearchService`：
  - 对接 Chroma query（向量或 metadata 过滤）。
  - 支持 tier 混合检索与 rerank（先简单打分）。
- 技能集成：
  - 在相关 skill 提示词中加入“先查 StructuredMemory 再执行”的策略。

### 为什么这么设计

- 没有查询就没有复用价值；先做最小查询工具让 agent/skill 真正消费 StructuredMemory。

### 对当前项目影响

- 增加工具组能力，可能影响模型工具选择概率，需要在 prompt 明确使用场景。

### upstream 合并冲突简案

- 工具定义走配置注册，避免硬编码在主工具列表。
- 若上游调整工具权限模型，按 group 维持隔离（如 `memory:business:read`）。

### 当前进度

- 未开始（与 MVP-1 **并列优先**，便于端到端验收）。

### 新增函数与配置清单（MVP-3 实现）

- `StructuredMemoryTool.query(query_text, tier_filter, tags, top_k, time_range)`（新增）
  - 作用：为 agent/skill 提供统一查询工具接口。
  - 设计理由：把复杂检索参数封装在工具层，降低 prompt 复杂度。
- `StructuredMemorySearchService.search(...)`（新增）
  - 作用：执行主检索流程（过滤 -> 召回 -> 排序 -> 裁剪）。
  - 设计理由：把 query 编排从 tool 中剥离，便于复用与测试。
- `StructuredMemorySearchService._build_where_filter(...)`（新增）
  - 作用：构建 metadata 过滤条件（tier/tags/user/agent/time）。
  - 设计理由：过滤逻辑集中后，避免不同调用方语义不一致。
- `StructuredMemorySearchService._rerank_results(...)`（新增）
  - 作用：综合向量分数、时间衰减、状态权重进行二次排序。
  - 设计理由：企业知识往往同时要求“相关性 + 时效性”。
- `StructuredMemorySearchService.format_for_agent(...)`（新增）
  - 作用：将检索结果格式化为 agent 可消费文本（含来源链路）。
  - 设计理由：统一输出样式，降低不同 skill 的接入成本。
- `structured_memory.query.default_top_k`（配置，新增）
  - 作用：设定默认召回条数。
  - 设计理由：保障稳定性能与可控 token 成本。
- `structured_memory.query.max_top_k`（配置，新增）
  - 作用：限制单次最大查询量。
  - 设计理由：避免大查询拖慢主链路。
- `structured_memory.query.default_tiers`（配置，新增）
  - 作用：设置默认检索层（如 `core + distilled`）。
  - 设计理由：默认避开 raw 噪声，提高首答质量。

---

## MVP-4：治理与演进（增强，不阻塞闭环）

### 需实现功能细节

- 冲突检测：相同主题多版本冲突标记。
- 生命周期治理：过期归档、软删除、重建索引。
- 可观测性：写入成功率、查询命中率；（启用 MVP-2 后）确认通过率。
- 可迁移性：抽象 `StructuredMemoryStore`，后续支持非 Chroma 后端。

### 为什么这么设计

- 企业场景里数据长期演进，治理能力决定可持续性。

### 对当前项目影响

- 运维与监控成本上升，但可显著提升长期稳定性与可审计性。

### upstream 合并冲突简案

- 将治理模块保持在 `deerflow/memory/structured_memory_*` 独立命名空间，减少与上游核心文件重叠。

### 当前进度

- 未开始。

### 新增函数与配置清单（MVP-4 实现）

- `StructuredMemoryGovernanceService.detect_conflicts(memory_ids)`（新增）
  - 作用：检测语义冲突或互斥事实并输出冲突组。
  - 设计理由：企业规则经常演进，冲突不可避免，需要自动预警。
- `StructuredMemoryGovernanceService.archive_expired(...)`（新增）
  - 作用：按 TTL 或状态将陈旧记录归档。
  - 设计理由：控制主库规模，提高查询质量。
- `StructuredMemoryGovernanceService.reindex(...)`（新增）
  - 作用：触发重建索引、修复 metadata 不一致。
  - 设计理由：长周期运行后必须有维护手段。
- `StructuredMemoryObservabilityService.emit_write_metrics(...)`（新增）
  - 作用：上报写入、确认、拒绝、查询命中指标。
  - 设计理由：没有指标就无法评估记忆系统价值。
- `StructuredMemoryStore`（抽象接口，新增）
  - 作用：抽象存储后端，定义 create/update/query/transaction 契约。
  - 设计理由：隔离 Chroma 细节，减少将来迁移代价。
- `structured_memory.governance.conflict_policy`（配置，新增）
  - 作用：冲突处理策略（保留新值/保留高置信/人工复核）。
  - 设计理由：不同业务线冲突策略不同，必须可配置。
- `structured_memory.governance.archive_ttl_days`（配置，新增）
  - 作用：归档时间窗口。
  - 设计理由：让治理策略参数化而非硬编码。
- `structured_memory.observability.enabled`（配置，新增）
  - 作用：治理指标开关。
  - 设计理由：在低成本环境可关闭，生产可开启。

---

## 6. 推荐代码落点（最小侵入）

- `backend/packages/harness/deerflow/memory/`
  - `structured_memory_models.py`（新增，可选：与现有 `models.py` 渐进合并）
  - `structured_memory_repository.py`（新增，可封装现有 `StructuredMemoryRepository`）
  - `structured_memory_services.py`（新增：**write + search** 优先；`commit` 随 MVP-2 再加）
- `backend/packages/harness/deerflow/tools/builtins/`
  - `structured_memory_write_tool.py`（新增）
  - `structured_memory_query_tool.py`（新增，或与写入合并为同一模块两工具）
- `backend/packages/harness/deerflow/agents/middlewares/`
  - `structured_memory_confirmation_middleware.py`（**暂缓**，随 MVP-2）
- `backend/packages/harness/deerflow/config/`
  - `structured_memory_config.py`（新增）
- `config.example.yaml`
  - 新增 `structured_memory` 配置段（新增）
- `backend/tests/`
  - `test_structured_memory_*.py`（新增）

---

## 7. 开发执行顺序（建议，已按当前策略调整）

1. **MVP-1**：agent 显式 **写入**工具 + `WriteService` + 配置护栏 + 单测。
2. **MVP-3**：**查询**工具 + `SearchService` + 相关 skill/prompt 接入建议 + 单测。
3. **业务验证**：在实际任务上观察写入频率、检索命中率、误写/噪声；再决定是否需要收紧 prompt、限制 `allowed_tiers`、或启用草案模式。
4. **MVP-2（可选增量）**：确认中间件 + `CommitService` + 多轮修订，与写入路径用配置切换或并存。
5. **MVP-4**：治理、观测、存储抽象。

该顺序保证**先打通「自主决策 + 工具写入 + 检索复用」**，再叠加审批与治理，符合当前「暂缓审批环境」的决策。
