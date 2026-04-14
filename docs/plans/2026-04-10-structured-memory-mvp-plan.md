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
- **契约精简**：在用户确认与记录治理均未上线前，**不强制**为每条记忆维护独立「状态机」类型；优先保证 **写入参数校验、来源字段、查询与配置护栏**。待 MVP-4（或重新启用 MVP-2）再引入显式状态或等价 metadata，避免过早抽象。

### 1.3 当前代码基线（用于设计决策）

- 已有会话记忆主链路：`MemoryMiddleware.after_agent()` -> `MemoryUpdateQueue.add()` -> `MemoryUpdater.update_memory()` -> `MemoryStorage.save()`。
- `MemoryStorage` 当前抽象的数据契约是 `dict[str, Any]` 的单体 memory payload（对应 `memory.json`）。
- 已有 Chroma 分层仓储基础：`deerflow.memory.models`、`deerflow.memory.repository.StructuredMemoryRepository`，支持 `raw/distilled/core` 三层的 create/get。
- 尚无面向 agent 的 **StructuredMemory 显式写入工具链**（规范化、按 tier 写入、来源字段）与 **统一查询工具**；**用户确认后提交**的编排（确认状态机、多轮修订）刻意后移，不在当前迭代范围。

### 1.4 分层数据契约（与 `deerflow.memory.models` 对齐）

- **共性**：`MemoryRecordCommon` 含 `id`、`title`（概括本条记忆）、`content`、`created_at` / `updated_at`、`tags`、`source_agent`、`user`。
- **Raw**：语义上**均来自同一会话线程**；必填 `source_thread_id`。对话内可能出现的**用户上传文件路径、图片路径、用户给出的 URL** 以列表形式可选挂载：`attachment_file_paths`、`attachment_image_paths`、`inline_web_urls`（可多值，也可为空）。由 agent 将对话与附件内容整理为**一条或多条** raw。
- **Distilled**：由一条或多条 raw 归纳而来；`raw_memory_ids` **至少 1 个**。
- **Core**：由一条或多条 distilled 进一步抽象而来；`distilled_memory_ids` **至少 1 个**（**不强制**多条 distilled，单条亦可）。
- **血缘**：distilled → raw、core → distilled 仅通过 ID 列表关联；更细粒度（如 message_id）可按需后续加 metadata，不阻塞当前契约。

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
  - StructuredMemory 需要 `create/query/list` 等面向记录的 API，不适合 `load/save(dict)`。（当前阶段以 `create` + `query` 为主；**按记录更新状态/软删**等与治理或确认流一并考虑，见 MVP-4 / 暂缓的 MVP-2。）

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
| `StructuredMemoryWriteTool`（或等价内置工具） | 新增 | agent 显式写入；**tier / title / content / tags** 及 **raw 会话锚点与附件列表** 或 **distilled/core 的 id 列表**由调用参数体现「自主决策」结果 | **MVP-1（当前优先）** |
| `StructuredMemoryWriteService.build_record(...)` | 新增 | 与原 `DraftService` 职责类似：规范化、来源、幂等、长度与类型校验 | **MVP-1** |
| `StructuredMemoryTool.query()` | 新增 | agent/skill 统一检索 | **MVP-3（当前优先，与写入并列验证）** |
| `StructuredMemoryConfirmationMiddleware` / `CommitService` / `mark_rejected` 等 | 新增 | 用户确认后再 promote | **暂缓（原 MVP-2，主流程验证后）** |
| `StructuredMemoryIndexService.update_indexes()` | 新增 | 检索优化、标签索引 | MVP-3/4 |
| `StructuredMemoryRecordStatus`（或等价生命周期语义） | 可选/暂缓 | 无确认且无治理前**可不建**；归档/软删/替代需要稳定语义时引入 | **MVP-4 首选**；MVP-2 启用时可先上**提案子状态** |
| 记忆生命周期治理（归档/冲突合并） | 新增 | 后续增强 | MVP-4 |

---

## 5. MVP 拆分与实施细节

## MVP-0（当前进度基线）

### 需实现/已有状态

- 已具备：
  - 会话记忆完整链路（自动提炼 + 注入）。
  - Chroma 分层 repository 基础数据结构与 create/get。
  - Clarification 通用能力（当前 StructuredMemory **不依赖**其做审批）。
  - **StructuredMemory 配置面（MVP-0，已落代码）**：`structured_memory.enabled` / `structured_memory.store`（仅 `chroma`）、`deerflow/config/structured_memory_config.py` 单例加载、`AppConfig.structured_memory` 与 YAML 同步、`config.example.yaml` 示例段（`config_version` 已随 schema 递增）、`get_structured_memory_repository()` 在 `enabled: false` 时抛出 `StructuredMemoryDisabledError`；单测见 `backend/tests/test_structured_memory_config.py`。
- 未具备（当前迭代要补）：
  - agent 显式 **写入** StructuredMemory 的工具与服务（含规范化与来源字段）。
  - 面向 agent/skill 的 **统一查询**工具。
- 刻意延后：
  - StructuredMemory **用户确认状态机**与「确认后再 promote」编排（原 MVP-2）。

### 设计原因

- 先承认已有基础，再做最小补齐，避免推倒现有 memory 体系。

### 项目影响

- 默认 `structured_memory.enabled: true` 时行为与此前一致（仍可正常获取 repository）。
- 显式关闭后：`get_structured_memory_repository()` 不可用（抛错），与会话记忆 `memory.enabled` **互不影响**。

### upstream 合并冲突简案

- 持续关注 `agents/memory/*`、`memory/*`、`deerflow/config/app_config.py` 与 `deerflow/config/*_config.py` 上游变更（本仓库已新增 `structured_memory_config.py`）。

### 当前进度

- **已完成**：`structured_memory.enabled` / `structured_memory.store` 配置模型与加载链；`config.example.yaml` 文档与版本号；仓储全局入口与开关语义；`reset_structured_memory_repository_singleton()` 供测试/热切换；`deerflow.config` / `deerflow.memory` 对外导出补充。
- **仍为缺口（按原优先级）**：MVP-1 写入工具与服务、MVP-3 统一查询工具（见下节）。

### 新增函数与配置清单（本阶段）

- **`StructuredMemoryRecordStatus`（枚举）：当前不纳入实现清单**
  - 无用户确认、无归档/软删/版本替代前，记录可**默认视为有效**，Chroma 文档**不必**携带 `status` 字段亦可跑通 MVP-1 / MVP-3。
  - **若落地 MVP-2**：再引入与确认流相关的状态（如 `draft` / `pending_confirmation` / `rejected`），可与提案 ID、修订次数等字段同批设计。
  - **若落地 MVP-4**：再引入与治理相关的状态（如 `active` / `archived` / `superseded`）；是否与确认态合并为同一枚举、或分域定义，**届时再选**，避免现阶段为「可能扩展」提前锁死命名。
- **`StructuredMemorySourceRef`：当前路线下不必实现，也不再作为「推荐项」**
  - **原因**：来源语义已由 `RawMemoryRecord` 的 **`source_thread_id` + `attachment_file_paths` / `attachment_image_paths` / `inline_web_urls`** 表达；distilled/core 以 **`raw_memory_ids` / `distilled_memory_ids`** 做血缘即可。再单立 `SourceRef` 会与 raw 模型**重复**，增加迁移与双写心智负担。
  - **何时才值得讨论**：若将来出现**与 raw 记录形状无关**、却要在多处在同一套 JSON 里传「出处」（例如仅工具协议层、或 distilled metadata 要强冗余全量出处且不想嵌整段 `RawMemoryRecord`），可再评估**抽值对象或别名类型**；名称也未必沿用 `StructuredMemorySourceRef`，以实际契约为准。
  - **结论**：MVP-1 / MVP-3 **不依赖**该类型；计划文中保留本条仅为**显式否定**早期文档里的「推荐引入」，避免后续读者误以为仍要开发。
- `structured_memory.enabled`（配置，**已实现**）
  - 作用：StructuredMemory 总开关，与现有 `memory.enabled` 解耦。
  - 设计理由：会话记忆与 StructuredMemory 要可独立启停，避免互相影响。
  - 落点：`StructuredMemoryConfig.enabled`、`load_structured_memory_config_from_dict`（`AppConfig.from_file` 每次刷新）、`get_structured_memory_repository()` 前置校验。
- `structured_memory.store`（配置，**已实现**）
  - 作用：声明 StructuredMemory 后端类型，首版固定 `chroma`（Pydantic `Literal`，非 chroma 值在加载时校验失败）。
  - 设计理由：为后续引入 PGVector/ES 预留扩展点。
  - 落点：`StructuredMemoryConfig.store`；当前仅 chroma 有运行时实现（`StructuredMemoryRepository`）。

**当前迭代最小配置面（实现 MVP-1 + MVP-3 时建议具备）**：`structured_memory.enabled`、`structured_memory.store`、以及 MVP-1/MVP-3 各节中的 `write.*` / `query.*` 护栏项；其余待确认流或治理阶段再扩展。

---

## MVP-1：agent 显式写入（当前优先）

### 需实现功能细节

- 新增 **`StructuredMemoryWriteTool`**（名称可微调，职责不变）：agent/skill 在需要沉淀知识时**主动调用**。
  - 输入（示例维度，以实现为准）：`tier`（`raw` / `distilled` / `core`）、`title`、`content`、`tags`；写 **raw** 时 **`source_thread_id`** 及可选 **`attachment_file_paths` / `attachment_image_paths` / `inline_web_urls`**；写 **distilled** 时 **`raw_memory_ids`（≥1）**；写 **core** 时 **`distilled_memory_ids`（≥1，不强制多条）**。
  - **无需**在首版工具参数中暴露「记录状态枚举」；治理态由后续 MVP-4 或元数据扩展承担。
  - 输出：`memory_id`、写入 tier、摘要；错误时返回可行动说明（缺字段、超长、非法 tier 组合等）。
  - **范围、类型与内容**不在工具内用硬编码策略代替模型判断，而由 **系统提示 + Skill** 约束「何时写、写什么层、写什么主题」；工具只做**校验与持久化**。
- 新增 **`StructuredMemoryWriteService`**（可与原 `DraftService` 合并命名，但职责以写入为主）：
  - 规范化正文与标签、长度上限；校验 raw 的 `source_thread_id` 必填（服务层不做默认推断）。
  - 幂等策略（可选，按 `content + source + tier`）当前暂缓，见下文「`compute_idempotency_key` 暂缓说明」。
  - 调用已有 `StructuredMemoryRepository.create_raw_memory` / `create_distilled_memory` / `create_core_memory`。
- 新增配置段（建议）：
  - `structured_memory.enabled`、`structured_memory.store`（**MVP-0 已在仓库落地**，见上节）
  - `structured_memory.write.max_content_length`（运维侧约束；**不等同**于代替模型决策）；可选：`max_attachment_refs_per_field` 等防止列表过长。
- 新增最小测试：
  - write tool/service 单测（各 tier 成功路径、校验失败路径）。
  - 与 Chroma 集成的冒烟测试（若已有 harness）。

### `compute_idempotency_key` 暂缓说明（本轮结论）

- **结论**：`compute_idempotency_key` 当前迭代暂不实现，后续按业务观测结果决定是否纳入 MVP-1 增量或并入 MVP-4 治理。
- **必要性（为什么值得做）**：
  - agent 自主写入阶段可能因重试、并发或重复推理触发同条 raw 的重复写入。
  - 后续若接入 update/delete 工具，提前降低重复噪声可减轻治理成本。
- **困难点（为什么先不做）**：
  - 同一会话允许多条 raw；`title`/`content` 由 agent 生成，存在改写与近似表达，误判会吞掉本应保留的新知识。
  - 轻量去重难以覆盖并发竞态；硬幂等通常需要额外唯一索引或事务能力，当前 Chroma 主存储路径实现成本较高。
  - 需要先用线上数据确定“严格重复”与“有效改写”的边界，避免过早固化规则。
- **推荐实现方案（后续落地参考）**：
  - 先做**轻量版本**：仅拦截严格重复，不做语义近似合并。
  - raw 判重 key 建议基于稳定字段：`tier + source_thread_id + normalized_content + sorted_attachments`（不依赖 `title`）。
  - 命中策略建议“短窗口去重 + 可观测日志”，并保留开关，后续再评估是否升级为强一致幂等（硬幂等）。

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

- **已完成（首批）**：
  - `StructuredMemoryWriteService.normalize_and_write(...)` 已落地：tier 校验、长度校验、来源/血缘字段校验、repository 写入分发。
  - `StructuredMemoryWriteTool` 已落地并注入 builtins：支持 raw/distilled/core 参数；raw 在未显式传 `source_thread_id` 时，先尝试从 runtime 获取当前 `thread_id`，再传入服务层。
  - `structured_memory.write.max_content_length` 已落地到配置模型与 `config.example.yaml`。
  - 单测已覆盖核心路径与失败路径（`backend/tests/test_structured_memory_write.py`）。
- **暂缓**：
  - `compute_idempotency_key`（见上文暂缓说明）。

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
  - 当前状态：**暂缓实现**，待业务观测后再决定轻量或硬幂等路线。
- `structured_memory.write.max_content_length`（配置，新增）
  - 作用：限制单条长度。
  - 设计理由：性能与成本护栏。
- `structured_memory.write.allowed_tiers`（配置，候选）
  - 作用：部署级允许写入的 tier 白名单。
  - 当前状态：**本轮不实现**，后续按运维需求再引入。
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
- 新增状态模型（建议，**仅在本节启用时需要**）
  - 提案/草案侧：`draft` / `pending_confirmation` / `confirmed` / `rejected` / `revised` 等；记录 `confirmed_by`、`confirmed_at`、`revision_count`。
  - 与 MVP-0 中「暂不实现 `StructuredMemoryRecordStatus`」不矛盾：此处为 **确认流专用子状态**；全局记录生命周期枚举仍可在 MVP-4 统一引入或与这里合并设计。
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
  - 作用：由一条或多条 distilled 合并/抽象进入 core，形成可稳定复用的事实层（`distilled_ids` 至少 1 条，产品策略决定是否偏好多条合并）。
  - 设计理由：在**启用 MVP-2** 的配置下，core 建议仅经确认后 promote；与 MVP-1「工具直写 core」可并存时，以 **`structured_memory` 配置**区分模式，避免语义混用。
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
  - 作用：综合向量分数、时间衰减、**tier 等 metadata** 进行二次排序（首版可不依赖记录级 `status` 字段；若后续写入 `archived` 等标记，再纳入过滤或降权）。
  - 设计理由：企业知识往往同时要求「相关性 + 时效性」；排序策略与是否存在状态枚举解耦。
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

- `StructuredMemoryRecordStatus`（或等价「记录生命周期」枚举/约定，**建议在本阶段首次落地**）
  - 作用：支撑归档、替代、软删、冲突标记等在查询与写入侧的可判定语义。
  - 设计理由：治理必须有稳定状态或等价 metadata；此前 MVP 刻意省略，至此再引入可避免空转抽象。
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
  - `structured_memory_models.py`（新增，可选：与现有 `models.py` 渐进合并；**首版可仅含写入/查询相关模型，状态枚举延至 MVP-4**）
  - `structured_memory_repository.py`（新增，可封装现有 `StructuredMemoryRepository`）
  - `structured_memory_services.py`（新增：**write + search** 优先；`commit` 随 MVP-2 再加）
- `backend/packages/harness/deerflow/tools/builtins/`
  - `structured_memory_write_tool.py`（新增）
  - `structured_memory_query_tool.py`（新增，或与写入合并为同一模块两工具）
- `backend/packages/harness/deerflow/agents/middlewares/`
  - `structured_memory_confirmation_middleware.py`（**暂缓**，随 MVP-2）
- `backend/packages/harness/deerflow/config/`
  - `structured_memory_config.py`（**MVP-0 已新增**：模型 + 单例 + `StructuredMemoryDisabledError`）
- `config.example.yaml`
  - `structured_memory` 配置段（**MVP-0 已写入示例**）
- `backend/tests/`
  - `test_structured_memory_config.py`（**MVP-0 已新增**）；后续 MVP-1/MVP-3 可继续扩展 `test_structured_memory_*.py`

---

## 7. 开发执行顺序（建议，已按当前策略调整）

0. **MVP-0（配置基线）**：`structured_memory.enabled` / `store` 与加载链 — **已完成**（见 MVP-0「当前进度」）。
1. **MVP-1**：agent 显式 **写入**工具 + `WriteService` + 配置护栏（`write.*` 等，叠在已有 `enabled`/`store` 之上）+ 单测。
2. **MVP-3**：**查询**工具 + `SearchService` + 相关 skill/prompt 接入建议 + 单测。
3. **业务验证**：在实际任务上观察写入频率、检索命中率、误写/噪声；再决定是否需要收紧 prompt、限制 `allowed_tiers`、或启用草案模式。
4. **MVP-2（可选增量）**：确认中间件 + `CommitService` + 多轮修订，与写入路径用配置切换或并存。
5. **MVP-4**：治理、观测、存储抽象。

该顺序保证**先打通「自主决策 + 工具写入 + 检索复用」**，再叠加审批与治理，符合当前「暂缓审批环境」的决策。
