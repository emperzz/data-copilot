# Data Warehouse Memory Summary

本文件用于记录历史所有会话的关键信息总结，作为优先查询的全局记忆入口。

## Query Priority

1. 先查询 `MEMORY.md`。
2. 仅当 `MEMORY.md` 有相关信息但不全面时，再查询 `memory/YYYY-MM-DD.md` 对应日期明细。

## Write Rules

- 每次会话结束后更新本文件，沉淀可跨会话复用的稳定事实。
- 仅记录关键结论，不写冗长过程。
- 当日存在会话时，必须创建或更新 `memory/YYYY-MM-DD.md` 记录当日细节。
- `MEMORY.md` 与 `memory/YYYY-MM-DD.md` 并行存在：
  - `MEMORY.md` 保存长期核心结论；
  - `memory/YYYY-MM-DD.md` 保存当日更完整细节，信息量应大于或等于 `MEMORY.md` 当日对应内容。
- 禁止仅写 `MEMORY.md` 而不写当日明细文件。

## When to write memory

- Decisions, preferences, and durable facts go to `MEMORY.md`.
- Day-to-day notes and running context go to `memory/YYYY-MM-DD.md`.
- If someone says "remember this," write it down (do not keep it in RAM).
- This area is still evolving. It helps to remind the model to store memories; it will know what to do.

## MEMORY.md Template（全局总结）

```markdown
## YYYY-MM-DD - <topic>
- Cross-Session Facts:
- Stable Business Rules:
- Confirmed Engine/Platform:
- Key Tables and Metrics:
- Reusable Decisions:
- Follow-up Risks:
```

## memory/YYYY-MM-DD.md Template（当日明细，按需创建）

文件命名格式：`YYYY-MM-DD.md`（例如 `2026-03-31.md`）。

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
