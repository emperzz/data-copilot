# DeerFlow 项目文档索引

`overview.md` 作为统一入口，汇总“架构”与“修改生效机制”两组文档，便于在 Cursor 中引用：

| 文档 | 说明 |
|------|------|
| [docs/architecture-overview.md](docs/architecture-overview.md) | 当前项目架构说明（分层、目录、模块关系等） |
| [docs/architecture-code-examples.md](docs/architecture-code-examples.md) | 与架构配套的代码摘录（Citations） |
| [docs/feature-development-suggestions.md](docs/feature-development-suggestions.md) | 新功能开发的策略与建议（参考，非唯一方案） |
| [modify.md](modify.md) | 代码修改生效机制文档入口 |
| [docs/modify-overview.md](docs/modify-overview.md) | 修改生效机制说明（热重载、重启策略、命令速查） |
| [docs/modify-code-examples.md](docs/modify-code-examples.md) | 与修改生效机制配套的代码摘录（Citations） |

任务执行时先读 `docs/architecture-overview.md`；涉及修改是否生效或是否重启时读 `modify.md` 并进入对应文档；需要细节时再查阅代码示例或仓库源码。若涉及新功能，可参考 `docs/feature-development-suggestions.md`，并结合用户实际需求与当前代码实现。
