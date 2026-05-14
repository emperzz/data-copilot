# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DeerFlow is a LangGraph-based AI super agent system with a full-stack architecture. The backend provides a "super agent" with sandbox execution, persistent memory, subagent delegation, and extensible tool integration - all operating in per-thread isolated environments.

**Architecture**:
- **Gateway API** (port 8001): REST API plus embedded LangGraph-compatible agent runtime
- **Frontend** (port 3000): Next.js web interface
- **Nginx** (port 2026): Unified reverse proxy entry point
- **Provisioner** (port 8002, optional in Docker dev): Started only when sandbox is configured for provisioner/Kubernetes mode

**Runtime**:
- `make dev`, Docker dev, and production all run the agent runtime in Gateway via `RunManager` + `run_agent()` + `StreamBridge` (`packages/harness/deerflow/runtime/`). Nginx exposes that runtime at `/api/langgraph/*` and rewrites it to Gateway's native `/api/*` routers.

**Project Structure**:
```
deer-flow/
├── Makefile                    # Root commands (check, install, dev, stop, etc.)
├── config.yaml                 # Main application configuration
├── extensions_config.json      # MCP servers and skills configuration
├── backend/
│   ├── Makefile               # Backend commands (dev, gateway, lint, test)
│   ├── langgraph.json         # LangGraph Studio graph configuration
│   ├── packages/harness/deerflow/
│   │   ├── agents/            # lead_agent, middlewares, memory, thread_state
│   │   ├── community/        # tavily, jina_ai, firecrawl, image_search, aio_sandbox, ddg_search, exa, infoquest
│   │   ├── config/            # app, model, sandbox, tool, memory, guardrails configs
│   │   ├── guardrails/       # guardrail provider system
│   │   ├── mcp/              # MCP integration (client, oauth, cache, tools)
│   │   ├── models/           # factory, vllm_provider, patched providers
│   │   ├── runtime/          # runs, serialization, store, stream_bridge
│   │   ├── sandbox/          # sandbox provider, tools, security, file_operation_lock
│   │   ├── skills/           # skills discovery, loading, tool_policy
│   │   ├── structured_memory/  # enterprise memory (models, storage, search, index_service)
│   │   ├── subagents/       # executor, registry, builtins (general-purpose, bash)
│   │   ├── tools/            # builtins (present_files, ask_clarification, view_image, task, setup_agent, update_agent, invoke_acp_agent, tool_search, structured_memory_tools, skill_manage)
│   │   ├── uploads/          # file upload handling
│   │   ├── tracing/          # tracing factory
│   │   ├── reflection/       # resolve_variable, resolve_class
│   │   ├── utils/            # network, readability, file_conversion
│   │   └── client.py         # DeerFlowClient
│   ├── app/
│   │   ├── gateway/          # FastAPI app + routers
│   │   └── channels/         # IM integrations (feishu, slack, telegram, dingtalk, discord, wechat, wecom)
│   ├── tests/
│   └── docs/
├── frontend/
└── skills/
```

## Important Development Guidelines

### Documentation Update Policy
**CRITICAL: Always update README.md and CLAUDE.md after every code change**

When making code changes, you MUST update the relevant documentation:
- Update `README.md` for user-facing changes (features, setup, usage instructions)
- Update `CLAUDE.md` for development changes (architecture, commands, workflows, internal systems)
- Keep documentation synchronized with the codebase at all times
- Ensure accuracy and timeliness of all documentation

## Commands

**Root directory** (for full application):
```bash
make setup           # Interactive setup wizard (recommended for new users)
make doctor          # Check configuration and system requirements
make config          # Generate local config files
make config-upgrade  # Merge new fields from config.example.yaml into config.yaml
make check           # Check if all required tools are installed
make install         # Install all dependencies (frontend + backend + pre-commit hooks)
make setup-sandbox   # Pre-pull sandbox container image
make dev             # Start all services in development mode (with hot-reloading)
make dev-daemon      # Start dev services in background (daemon mode)
make start           # Start all services in production mode
make start-daemon    # Start prod services in background (daemon mode)
make stop            # Stop all running services
make clean           # Clean up processes and temporary files
make up              # Build and start production Docker services
make down            # Stop and remove production Docker containers
make docker-init     # Pull the sandbox image
make docker-start    # Start Docker development environment
make docker-stop     # Stop Docker development environment
make docker-logs     # View Docker development logs
```

**Backend directory** (for backend development only):
```bash
make install    # Install backend dependencies
make dev        # Run Gateway API with reload (port 8001)
make gateway    # Run Gateway API only (port 8001)
make test       # Run all backend tests
make lint       # Lint with ruff
make format     # Format code with ruff
```

Regression tests related to Docker/provisioner behavior:
- `tests/test_docker_sandbox_mode_detection.py` (mode detection from `config.yaml`)
- `tests/test_provisioner_kubeconfig.py` (kubeconfig file/directory handling)

Boundary check (harness → app import firewall):
- `tests/test_harness_boundary.py` — ensures `packages/harness/deerflow/` never imports from `app.*`

CI runs these regression tests for every pull request via [.github/workflows/backend-unit-tests.yml](../.github/workflows/backend-unit-tests.yml).

## Architecture

### Harness / App Split

The backend is split into two layers with a strict dependency direction:

- **Harness** (`packages/harness/deerflow/`): Publishable agent framework package (`deerflow-harness`). Import prefix: `deerflow.*`. Contains agent orchestration, tools, sandbox, models, MCP, skills, config — everything needed to build and run agents.
- **App** (`app/`): Unpublished application code. Import prefix: `app.*`. Contains the FastAPI Gateway API and IM channel integrations (Feishu, Slack, Telegram, DingTalk, Discord, WeChat, WeCom).

**Dependency rule**: App imports deerflow, but deerflow never imports app. This boundary is enforced by `tests/test_harness_boundary.py` which runs in CI.

**Import conventions**:
```python
# Harness internal
from deerflow.agents import make_lead_agent
from deerflow.models import create_chat_model

# App internal
from app.gateway.app import app
from app.channels.service import start_channel_service

# App → Harness (allowed)
from deerflow.config import get_app_config

# Harness → App (FORBIDDEN — enforced by test_harness_boundary.py)
# from app.gateway.routers.uploads import ...  # ← will fail CI
```

### Agent System

**Lead Agent** (`packages/harness/deerflow/agents/lead_agent/agent.py`):
- Entry point: `make_lead_agent(config: RunnableConfig)` registered in `langgraph.json`
- Dynamic model selection via `create_chat_model()` with thinking/vision support
- Tools loaded via `get_available_tools()` - combines sandbox, built-in, MCP, community, and subagent tools
- System prompt generated by `apply_prompt_template()` with skills, memory, and subagent instructions

**ThreadState** (`packages/harness/deerflow/agents/thread_state.py`):
- Extends `AgentState` with: `sandbox`, `thread_data`, `title`, `artifacts`, `todos`, `uploaded_files`, `viewed_images`
- Uses custom reducers: `merge_artifacts` (deduplicate), `merge_viewed_images` (merge/clear)

**Runtime Configuration** (via `config.configurable`):
- `thinking_enabled` - Enable model's extended thinking
- `model_name` - Select specific LLM model
- `is_plan_mode` - Enable TodoList middleware
- `subagent_enabled` - Enable task delegation tool

### Middleware Chain

Middleware chain is assembled in two places: `build_lead_runtime_middlewares()` (shared base) and `_build_middlewares()` (lead-agent specific). Order matters.

**Base middlewares** (`tool_error_handling_middleware.py` → `build_lead_runtime_middlewares`):
1. **ThreadDataMiddleware** - Creates per-thread directories; resolves `user_id` via `get_effective_user_id()` (falls back to `"default"` in no-auth mode)
2. **UploadsMiddleware** - Tracks and injects newly uploaded files into conversation
3. **SandboxMiddleware** - Acquires sandbox, stores `sandbox_id` in state

4. **DanglingToolCallMiddleware** - Injects placeholder ToolMessages for AIMessage tool_calls that lack responses (e.g., due to user interruption), including raw provider tool-call payloads preserved only in `additional_kwargs["tool_calls"]`
5. **LLMErrorHandlingMiddleware** - Normalizes provider/model invocation failures into recoverable assistant-facing errors before later middleware/tool stages run
6. **GuardrailMiddleware** - Pre-tool-call authorization via pluggable `GuardrailProvider` protocol (optional, if `guardrails.enabled` in config). Evaluates each tool call and returns error ToolMessage on deny. Three provider options: built-in `AllowlistProvider` (zero deps), OAP policy providers (e.g. `aport-agent-guardrails`), or custom providers. See [docs/GUARDRAILS.md](docs/GUARDRAILS.md) for setup, usage, and how to implement a provider.
7. **SandboxAuditMiddleware** - Audits sandboxed shell/file operations for security logging before tool execution continues
8. **ToolErrorHandlingMiddleware** - Converts tool exceptions into error `ToolMessage`s so the run can continue instead of aborting
9. **SummarizationMiddleware** - Context reduction when approaching token limits (optional, if enabled)
10. **TodoListMiddleware** - Task tracking with `write_todos` tool (optional, if plan_mode)
11. **TokenUsageMiddleware** - Records token usage metrics when token tracking is enabled (optional); subagent usage is cached by `tool_call_id` only while token usage is enabled and merged back into the dispatching AIMessage by message position rather than message id
12. **TitleMiddleware** - Auto-generates thread title after first complete exchange and normalizes structured message content before prompting the title model
13. **MemoryMiddleware** - Queues conversations for async memory update (filters to user + final AI responses)
14. **ViewImageMiddleware** - Injects base64 image data before LLM call (conditional on vision support)
15. **DeferredToolFilterMiddleware** - Hides deferred tool schemas from the bound model until tool search is enabled (optional)
16. **SubagentLimitMiddleware** - Truncates excess `task` tool calls from model response to enforce `MAX_CONCURRENT_SUBAGENTS` limit (optional, if `subagent_enabled`)
17. **LoopDetectionMiddleware** - Detects repeated tool-call loops; hard-stop responses clear both structured `tool_calls` and raw provider tool-call metadata before forcing a final text answer
18. **ClarificationMiddleware** - Intercepts `ask_clarification` tool calls, interrupts via `Command(goto=END)` (must be last)


### Configuration System

**Main Configuration** (`config.yaml`):

Setup: Copy `config.example.yaml` to `config.yaml` in the **project root** directory.

**Config Versioning**: `config.example.yaml` has a `config_version` field. On startup, `AppConfig.from_file()` compares user version vs example version and emits a warning if outdated. Missing `config_version` = version 0. Run `make config-upgrade` to auto-merge missing fields. When changing the config schema, bump `config_version` in `config.example.yaml`.

**Config Caching**: `get_app_config()` caches the parsed config, but automatically reloads it when the resolved config path changes or the file's mtime increases. This keeps Gateway and LangGraph reads aligned with `config.yaml` edits without requiring a manual process restart.

Configuration priority:
1. Explicit `config_path` argument
2. `DEER_FLOW_CONFIG_PATH` environment variable
3. `config.yaml` in current directory (backend/)
4. `config.yaml` in parent directory (project root - **recommended location**)

Config values starting with `$` are resolved as environment variables (e.g., `$OPENAI_API_KEY`).
`ModelConfig` also declares `use_responses_api` and `output_version` so OpenAI `/v1/responses` can be enabled explicitly while still using `langchain_openai:ChatOpenAI`.

**Extensions Configuration** (`extensions_config.json`):

MCP servers and skills are configured together in `extensions_config.json` in project root:

Configuration priority:
1. Explicit `config_path` argument
2. `DEER_FLOW_EXTENSIONS_CONFIG_PATH` environment variable
3. `extensions_config.json` in current directory (backend/)
4. `extensions_config.json` in parent directory (project root - **recommended location**)

### Gateway API (`app/gateway/`)

FastAPI application on port 8001 with health check at `GET /health`. Set `GATEWAY_ENABLE_DOCS=false` to disable `/docs`, `/redoc`, and `/openapi.json` in production (default: enabled).

CORS is same-origin by default when requests enter through nginx on port 2026. Split-origin or port-forwarded browser clients must opt in with `GATEWAY_CORS_ORIGINS` (comma-separated exact origins); Gateway `CORSMiddleware` and `CSRFMiddleware` both read that variable so browser CORS and auth-origin checks stay aligned.

**Routers**:

| Router | Endpoints |
|--------|-----------|
| **Models** (`/api/models`) | `GET /` - list models; `GET /{name}` - model details |
| **MCP** (`/api/mcp`) | `GET /config` - get config; `PUT /config` - update config (saves to extensions_config.json) |
| **Skills** (`/api/skills`) | `GET /` - list skills; `GET /{name}` - details; `PUT /{name}` - update enabled; `POST /install` - install from .skill archive (accepts standard optional frontmatter like `version`, `author`, `compatibility`) |
| **Memory** (`/api/memory`) | `GET /` - memory data; `POST /reload` - force reload; `GET /config` - config; `GET /status` - config + data |
| **Uploads** (`/api/threads/{id}/uploads`) | `POST /` - upload files (auto-converts PDF/PPT/Excel/Word); `GET /list` - list; `DELETE /{filename}` - delete |
| **Threads** (`/api/threads/{id}`) | `DELETE /` - remove DeerFlow-managed local thread data after LangGraph thread deletion; unexpected failures are logged server-side and return a generic 500 detail |
| **Artifacts** (`/api/threads/{id}/artifacts`) | `GET /{path}` - serve artifacts; active content types (`text/html`, `application/xhtml+xml`, `image/svg+xml`) are always forced as download attachments to reduce XSS risk; `?download=true` still forces download for other file types |
| **Suggestions** (`/api/threads/{id}/suggestions`) | `POST /` - generate follow-up questions; rich list/block model content is normalized before JSON parsing |
| **Thread Runs** (`/api/threads/{id}/runs`) | `POST /` - create background run; `POST /stream` - create + SSE stream; `POST /wait` - create + block; `GET /` - list runs; `GET /{rid}` - run details; `POST /{rid}/cancel` - cancel; `GET /{rid}/join` - join SSE; `GET /{rid}/messages` - paginated messages `{data, has_more}`; `GET /{rid}/events` - full event stream; `GET /../messages` - thread messages with feedback; `GET /../token-usage` - aggregate tokens |
| **Feedback** (`/api/threads/{id}/runs/{rid}/feedback`) | `PUT /` - upsert feedback; `DELETE /` - delete user feedback; `POST /` - create feedback; `GET /` - list feedback; `GET /stats` - aggregate stats; `DELETE /{fid}` - delete specific |
| **Runs** (`/api/runs`) | `POST /stream` - stateless run + SSE; `POST /wait` - stateless run + block; `GET /{rid}/messages` - paginated messages by run_id `{data, has_more}` (cursor: `after_seq`/`before_seq`); `GET /{rid}/feedback` - list feedback by run_id |

Proxied through nginx: `/api/langgraph/*` → Gateway LangGraph-compatible runtime, all other `/api/*` → Gateway REST APIs.

### Sandbox System (`packages/harness/deerflow/sandbox/`)

**Interface**: Abstract `Sandbox` with `execute_command`, `read_file`, `write_file`, `list_dir`
**Provider Pattern**: `SandboxProvider` with `acquire`, `get`, `release` lifecycle
**Implementations**:
- `LocalSandboxProvider` - Singleton local filesystem execution with path mappings
- `AioSandboxProvider` (`packages/harness/deerflow/community/`) - Docker-based isolation

**Virtual Path System**:
- Agent sees: `/mnt/user-data/{workspace,uploads,outputs}`, `/mnt/skills`
- Physical: `backend/.deer-flow/users/{user_id}/threads/{thread_id}/user-data/...`, `deer-flow/skills/`
- Translation: `replace_virtual_path()` / `replace_virtual_paths_in_command()`
- Detection: `is_local_sandbox()` checks `sandbox_id == "local"`

**Sandbox Tools** (in `packages/harness/deerflow/sandbox/tools.py`):
- `bash` - Execute commands with path translation and error handling
- `ls` - Directory listing (tree format, max 2 levels)
- `read_file` - Read file contents with optional line range
- `write_file` - Write/append to files, creates directories; overwrites by default and exposes the `append` argument in the model-facing schema for end-of-file writes
- `str_replace` - Substring replacement (single or all occurrences); same-path serialization is scoped to `(sandbox.id, path)` so isolated sandboxes do not contend on identical virtual paths inside one process

### Subagent System (`packages/harness/deerflow/subagents/`)

**Built-in Agents**: `general-purpose` (all tools except `task`) and `bash` (command specialist)
**Execution**: Dual thread pool - `_scheduler_pool` (3 workers) + `_execution_pool` (3 workers)
**Concurrency**: `MAX_CONCURRENT_SUBAGENTS = 3` enforced by `SubagentLimitMiddleware` (truncates excess tool calls in `after_model`), 15-minute timeout
**Flow**: `task()` tool → `SubagentExecutor` → background thread → poll 5s → SSE events → result
**Events**: `task_started`, `task_running`, `task_completed`/`task_failed`/`task_timed_out`

### Tool System (`packages/harness/deerflow/tools/`)

`get_available_tools(groups, include_mcp, model_name, subagent_enabled)` assembles:
1. **Config-defined tools** - Resolved from `config.yaml` via `resolve_variable()`
2. **MCP tools** - From enabled MCP servers (lazy initialized, cached with mtime invalidation)
3. **Built-in tools**:
   - `present_files` - Make output files visible to user (only `/mnt/user-data/outputs`)
   - `ask_clarification` - Request clarification (intercepted by ClarificationMiddleware → interrupts)
   - `view_image` - Read image as base64 (added only if model supports vision)
   - `setup_agent` - Bootstrap-only: persist a brand-new custom agent's `SOUL.md` and `config.yaml`. Bound only when `is_bootstrap=True`.
   - `update_agent` - Custom-agent-only: persist self-updates to the current agent's `SOUL.md` / `config.yaml` from inside a normal chat (partial update + atomic write). Bound when `agent_name` is set and `is_bootstrap=False`.
4. **Subagent tool** (if enabled):
   - `task` - Delegate to subagent (description, prompt, subagent_type)

**Community tools** (`packages/harness/deerflow/community/`):
- `tavily/` - Web search (5 results default) and web fetch (4KB limit)
- `jina_ai/` - Web fetch via Jina reader API with readability extraction
- `firecrawl/` - Web scraping via Firecrawl API

**ACP agent tools**:
- `invoke_acp_agent` - Invokes external ACP-compatible agents from `config.yaml`
- ACP launchers must be real ACP adapters. The standard `codex` CLI is not ACP-compatible by itself; configure a wrapper such as `npx -y @zed-industries/codex-acp` or an installed `codex-acp` binary
- Missing ACP executables now return an actionable error message instead of a raw `[Errno 2]`
- Each ACP agent uses a per-thread workspace at `{base_dir}/users/{user_id}/threads/{thread_id}/acp-workspace/`. The workspace is accessible to the lead agent via the virtual path `/mnt/acp-workspace/` (read-only). In docker sandbox mode, the directory is volume-mounted into the container at `/mnt/acp-workspace` (read-only); in local sandbox mode, path translation is handled by `tools.py`
- `image_search/` - Image search via DuckDuckGo

### MCP System (`packages/harness/deerflow/mcp/`)

- Uses `langchain-mcp-adapters` `MultiServerMCPClient` for multi-server management
- **Lazy initialization**: Tools loaded on first use via `get_cached_mcp_tools()`
- **Cache invalidation**: Detects config file changes via mtime comparison
- **Transports**: stdio (command-based), SSE, HTTP
- **OAuth (HTTP/SSE)**: Supports token endpoint flows (`client_credentials`, `refresh_token`) with automatic token refresh + Authorization header injection
- **Runtime updates**: Gateway API saves to extensions_config.json; LangGraph detects via mtime

### Skills System (`packages/harness/deerflow/skills/`)

- **Location**: `deer-flow/skills/{public,custom}/`
- **Format**: Directory with `SKILL.md` (YAML frontmatter: name, description, license, allowed-tools)
- **Loading**: `load_skills()` recursively scans `skills/{public,custom}` for `SKILL.md`, parses metadata, and reads enabled state from extensions_config.json
- **Injection**: Enabled skills listed in agent system prompt with container paths
- **Installation**: `POST /api/skills/install` extracts .skill ZIP archive to custom/ directory

### Model Factory (`packages/harness/deerflow/models/factory.py`)

- `create_chat_model(name, thinking_enabled)` instantiates LLM from config via reflection
- Supports `thinking_enabled` flag with per-model `when_thinking_enabled` overrides
- Supports vLLM-style thinking toggles via `when_thinking_enabled.extra_body.chat_template_kwargs.enable_thinking` for Qwen reasoning models, while normalizing legacy `thinking` configs for backward compatibility
- Supports `supports_vision` flag for image understanding models
- Config values starting with `$` resolved as environment variables
- Missing provider modules surface actionable install hints from reflection resolvers (for example `uv add langchain-google-genai`)

### vLLM Provider (`packages/harness/deerflow/models/vllm_provider.py`)

- `VllmChatModel` subclasses `langchain_openai:ChatOpenAI` for vLLM 0.19.0 OpenAI-compatible endpoints
- Preserves vLLM's non-standard assistant `reasoning` field on full responses, streaming deltas, and follow-up tool-call turns
- Designed for configs that enable thinking through `extra_body.chat_template_kwargs.enable_thinking` on vLLM 0.19.0 Qwen reasoning models, while accepting the older `thinking` alias

### IM Channels System (`app/channels/`)

Bridges external messaging platforms (Feishu, Slack, Telegram, DingTalk, Discord, WeChat, WeCom) to the DeerFlow agent via the LangGraph Server.

**Components**: message_bus (async pub/sub hub), store (JSON-file persistence), manager (core dispatcher), base (abstract Channel), service (lifecycle management), and platform-specific implementations.

**Message Flow**: External platform → Channel → `MessageBus.publish_inbound()` → `ChannelManager._dispatch_loop()` → create/lookup thread via Gateway API → `runs.stream()` or `runs.wait()` → accumulate response → outbound via channel callbacks → platform reply.

**Configuration** (`config.yaml` → `channels`): `langgraph_url`, `gateway_url`, and per-channel configs (app_id/app_secret for Feishu, bot_token for Slack/Telegram, client_id/client_secret for DingTalk, etc.). See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for details.


### Memory System (`packages/harness/deerflow/agents/memory/`)

**Components**:
- `updater.py` - LLM-based memory updates with fact extraction, whitespace-normalized fact deduplication (trims leading/trailing whitespace before comparing), and atomic file I/O
- `queue.py` - Debounced update queue (per-thread deduplication, configurable wait time); captures `user_id` at enqueue time so it survives the `threading.Timer` boundary
- `prompt.py` - Prompt templates for memory updates
- `storage.py` - File-based storage with per-user isolation; cache keyed by `(user_id, agent_name)` tuple

**Per-User Isolation**:
- Memory is stored per-user at `{base_dir}/users/{user_id}/memory.json`
- Per-agent per-user memory at `{base_dir}/users/{user_id}/agents/{agent_name}/memory.json`
- Custom agent definitions (`SOUL.md` + `config.yaml`) are also per-user at `{base_dir}/users/{user_id}/agents/{agent_name}/`. The legacy shared layout `{base_dir}/agents/{agent_name}/` remains read-only fallback for unmigrated installations
- `user_id` is resolved via `get_effective_user_id()` from `deerflow.runtime.user_context`
- In no-auth mode, `user_id` defaults to `"default"` (constant `DEFAULT_USER_ID`)
- Absolute `storage_path` in config opts out of per-user isolation
- **Migration**: Run `PYTHONPATH=. python scripts/migrate_user_isolation.py` to move legacy `memory.json`, `threads/`, and `agents/` into per-user layout. Supports `--dry-run` and `--user-id USER_ID`.

**Data Structure** (stored in `{base_dir}/users/{user_id}/memory.json`):
- **User Context**: `workContext`, `personalContext`, `topOfMind` (1-3 sentence summaries)
- **History**: `recentMonths`, `earlierContext`, `longTermBackground`
- **Facts**: Discrete facts with `id`, `content`, `category` (preference/knowledge/context/behavior/goal), `confidence` (0-1), `createdAt`, `source`

**Configuration** (`config.yaml` → `memory`):
- `enabled` / `injection_enabled` - Master switches
- `storage_path` - Path to memory.json (absolute path opts out of per-user isolation)
- `debounce_seconds` - Wait time before processing (default: 30)
- `model_name` - LLM for updates (null = default model)
- `max_facts` / `fact_confidence_threshold` - Fact storage limits (100 / 0.7)
- `max_injection_tokens` - Token limit for prompt injection (2000)

### Enterprise Structured Memory (`packages/harness/deerflow/structured_memory/`)

File-based knowledge management system for data warehouse metadata, business definitions, and task history. Complementary to the LLM-based Memory System above — structured memory is agent-managed markdown with explicit schema, while the Memory System uses LLM extraction.

**Directory Structure** (under `{base_dir}/structured_memory/`):
- `facts/warehouse/` — Table entities (ods, dwd, dws, dim, etc.)
- `facts/technical/` — Technical specifications (naming conventions, parameter definitions)
- `facts/business/` — Business definitions (metrics, funnels, business rules)
- `tasks/` — Task history organized by year
- `core.md` — Top-level summary injected into system prompt

**Core File** (`core.md`):
- Auto-created with a template on first access when `structured_memory.enabled = true`
- Contains domain-level summaries; full entity details loaded on demand via tools
- Injected into system prompt via `_get_structured_memory_context()` in `prompt.py`

**Components**:
- `models.py` - Pydantic models (`TableEntity`, `BusinessEntity`) with YAML frontmatter serialization, markdown parsing, and partial update helpers (`apply_partial_update`, `apply_partial_update_for_business`)
- `storage.py` - `StructuredMemoryStore` with atomic writes (temp file + rename), path traversal protection (`resolve_path` + `is_relative_to`), thread-safe singleton
- `index_service.py` - Auto-indexing: registers/unregisters entities in `index.md` files on create/update/delete; uses `fcntl` file locking (Linux) or `_NoOpLock` (Windows)
- `search.py` - Full-text search across `.md` files with substring matching and line snippets
- `templates.py` - Markdown templates for all entity types and index files

**Tools** (`tools/builtins/structured_memory_tools.py`):
- `search_structured_memory` - Keyword search across memory files
- `get_memory_entity` - Read full entity content by relative path
- `list_memory_entities` - Tree-formatted directory listing
- `create_memory_entity` - Create new entity with auto-registration
- `update_memory_entity` - Partial update (parse → mutate model → re-serialize); auto-updates index
- `delete_memory_entity` - Delete entity with auto-unregistration

**Data Flow for Index Updates**:
1. Tool calls `register_entity(store, path, old_content, new_content)` or `unregister_entity(store, path, content)`
2. `_acquire_index_lock()` locks the target `index.md` via `fcntl.LOCK_EX`
3. Entry parsed via `parse_entity_entry()` (extracts title from `**table**:` or first `# heading`)
4. `_update_index()` adds/removes/replaces the list item
5. `_release_index_lock()` releases and removes the `.lock` file

**Configuration** (`config.yaml` → `structured_memory`):
- `enabled` - Master switch
- `storage_path` - Defaults to `{base_dir}/structured_memory/`
- `injection_enabled` - Whether to inject `core.md` summary into system prompt
- `max_index_tokens` - Token budget for `core.md` injection (default: 1500)

### Reflection System

- `resolve_variable(path)` - Import module and return variable (e.g., `module.path:variable_name`)
- `resolve_class(path, base_class)` - Import and validate class against base class

### Config Schema

See `config.example.yaml` in the project root for the authoritative configuration schema. Key sections: `models[]`, `tools[]`, `tool_groups[]`, `sandbox`, `skills`, `title`, `summarization`, `subagents`, `memory`, `structured_memory`, `channels`.

`extensions_config.json` configures MCP servers (`mcpServers`) and skills (`skills`) states. Both config files can be modified at runtime via Gateway API endpoints or `DeerFlowClient` methods.

### Embedded Client (`packages/harness/deerflow/client.py`)

`DeerFlowClient` provides direct in-process access to all DeerFlow capabilities without HTTP services. All return types align with the Gateway API response schemas.

**Agent Conversation**: `chat(message, thread_id)` (synchronous) and `stream(message, thread_id)` (streaming with `values`, `messages-tuple`, `custom`, `end` events). See [docs/STREAMING.md](docs/STREAMING.md) for full design.

**Gateway Equivalent Methods**: `list_models()`, `get_model()`, `get_mcp_config()`, `update_mcp_config()`, `list_skills()`, `get_skill()`, `update_skill()`, `install_skill()`, `get_memory()`, `reload_memory()`, `get_memory_config()`, `get_memory_status()`, `upload_files()`, `list_uploads()`, `delete_upload()`, `get_artifact()`. See [docs/API.md](docs/API.md) for details.

**Tests**: `tests/test_client.py` (77 unit tests including `TestGatewayConformance`), `tests/test_client_live.py` (live integration tests).

## Development Workflow

### Test-Driven Development (TDD) — MANDATORY

**Every new feature or bug fix MUST be accompanied by unit tests. No exceptions.**

- Write tests in `backend/tests/` following the existing naming convention `test_<feature>.py`
- Run the full suite before and after your change: `make test`
- Tests must pass before a feature is considered complete
- For lightweight config/utility modules, prefer pure unit tests with no external dependencies
- If a module causes circular import issues in tests, add a `sys.modules` mock in `tests/conftest.py` (see existing example for `deerflow.subagents.executor`)

```bash
# Run all tests
make test

# Run a specific test file
PYTHONPATH=. uv run pytest tests/test_<feature>.py -v
```

### Running the Full Application

From the **project root** directory:
```bash
make dev
```

This starts all services and makes the application available at `http://localhost:2026`.

**All startup modes:**

| | **Local Foreground** | **Local Daemon** | **Docker Dev** | **Docker Prod** |
|---|---|---|---|---|
| **Dev** | `./scripts/serve.sh --dev`<br/>`make dev` | `./scripts/serve.sh --dev --daemon`<br/>`make dev-daemon` | `./scripts/docker.sh start`<br/>`make docker-start` | — |
| **Prod** | `./scripts/serve.sh --prod`<br/>`make start` | `./scripts/serve.sh --prod --daemon`<br/>`make start-daemon` | — | `./scripts/deploy.sh`<br/>`make up` |

| Action | Local | Docker Dev | Docker Prod |
|---|---|---|---|
| **Stop** | `./scripts/serve.sh --stop`<br/>`make stop` | `./scripts/docker.sh stop`<br/>`make docker-stop` | `./scripts/deploy.sh down`<br/>`make down` |
| **Restart** | `./scripts/serve.sh --restart [flags]` | `./scripts/docker.sh restart` | — |

**Nginx routing**:
- `/api/langgraph/*` → Gateway embedded runtime (8001), rewritten to `/api/*`
- `/api/*` (other) → Gateway API (8001)
- `/` (non-API) → Frontend (3000)

### Running Backend Services Separately

From the **backend** directory:

```bash
# Gateway API
make gateway
```

Direct access (without nginx):
- Gateway: `http://localhost:8001`

### Frontend Configuration

The frontend uses environment variables to connect to backend services:
- `NEXT_PUBLIC_LANGGRAPH_BASE_URL` - Defaults to `/api/langgraph` (through nginx)
- `NEXT_PUBLIC_BACKEND_BASE_URL` - Defaults to empty string (through nginx)

When using `make dev` from root, the frontend automatically connects through nginx.

## Key Features

**File Upload**: Multi-file upload with automatic PDF/PPT/Excel/Word conversion. See [docs/FILE_UPLOAD.md](docs/FILE_UPLOAD.md).
- Endpoint: `POST /api/threads/{thread_id}/uploads`
- Supports: PDF, PPT, Excel, Word documents (converted via `markitdown`)
- Rejects directory inputs before copying so uploads stay all-or-nothing
- Reuses one conversion worker per request when called from an active event loop
- Files stored in thread-isolated directories
- Duplicate filenames in a single upload request are auto-renamed with `_N` suffixes so later files do not truncate earlier files
- Agent receives uploaded file list via `UploadsMiddleware`

**Plan Mode**: TodoList middleware for complex multi-step tasks via `write_todos` tool. See [docs/plan_mode_usage.md](docs/plan_mode_usage.md).

**Context Summarization**: Automatic conversation summarization when approaching token limits. See [docs/summarization.md](docs/summarization.md).

**Vision Support**: For models with `supports_vision: true`, images are automatically converted to base64 and injected into state via `ViewImageMiddleware`.

**Enterprise Structured Memory**: File-based knowledge management system for data warehouse metadata, business definitions, and task history. See `structured_memory/` module and [docs/structured-memory-design.md](docs/structured-memory-design.md).

## Code Style

- Uses `ruff` for linting and formatting
- Line length: 240 characters
- Python 3.12+ with type hints
- Double quotes, space indentation

## Documentation

See `docs/` directory for detailed documentation:
- [CONFIGURATION.md](docs/CONFIGURATION.md) - Configuration options
- [ARCHITECTURE.md](docs/ARCHITECTURE.md) - Architecture details
- [API.md](docs/API.md) - API reference
- [SETUP.md](docs/SETUP.md) - Setup guide
- [FILE_UPLOAD.md](docs/FILE_UPLOAD.md) - File upload feature
- [PATH_EXAMPLES.md](docs/PATH_EXAMPLES.md) - Path types and usage
- [summarization.md](docs/summarization.md) - Context summarization
- [plan_mode_usage.md](docs/plan_mode_usage.md) - Plan mode with TodoList
