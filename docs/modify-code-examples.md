# 修改生效机制代码示例（原文摘录）

> 与 `docs/modify-overview.md` 配套的代码片段摘录；若与仓库代码不一致，以仓库为准。

### Citations

**File:** scripts/serve.sh (L116-122)
```shellscript
if $DEV_MODE; then
    LANGGRAPH_EXTRA_FLAGS=""
    GATEWAY_EXTRA_FLAGS="--reload --reload-include='*.yaml' --reload-include='.env'"
else
    LANGGRAPH_EXTRA_FLAGS="--no-reload"
    GATEWAY_EXTRA_FLAGS=""
fi
```

**File:** scripts/serve.sh (L124-125)
```shellscript
echo "Starting LangGraph server..."
(cd backend && NO_COLOR=1 uv run langgraph dev --no-browser --allow-blocking $LANGGRAPH_EXTRA_FLAGS > ../logs/langgraph.log 2>&1) &
```

**File:** frontend/package.json (L10-11)
```json
    "dev": "next dev --turbo",
    "lint": "eslint . --ext .ts,.tsx",
```

**File:** frontend/src/core/i18n/locales/zh-CN.ts (L14-50)
```typescript
export const zhCN: Translations = {
  // Locale meta
  locale: {
    localName: "中文",
  },

  // Common
  common: {
    home: "首页",
    settings: "设置",
    delete: "删除",
    rename: "重命名",
    share: "分享",
    openInNewWindow: "在新窗口打开",
    close: "关闭",
    more: "更多",
    search: "搜索",
    download: "下载",
    thinking: "思考",
    artifacts: "文件",
    public: "公共",
    custom: "自定义",
    notAvailableInDemoMode: "在演示模式下不可用",
    loading: "加载中...",
    version: "版本",
    lastUpdated: "最后更新",
    code: "代码",
    preview: "预览",
    cancel: "取消",
    save: "保存",
    install: "安装",
    create: "创建",
    export: "导出",
    exportAsMarkdown: "导出为 Markdown",
    exportAsJSON: "导出为 JSON",
    exportSuccess: "对话已导出",
  },
```

**File:** frontend/src/app/layout.tsx (L10-13)
```typescript
export const metadata: Metadata = {
  title: "DeerFlow",
  description: "A LangChain-based framework for building super agents.",
};
```

**File:** backend/packages/harness/deerflow/config/app_config.py (L257-291)
```python
def get_app_config() -> AppConfig:
    """Get the DeerFlow config instance.

    Returns a cached singleton instance and automatically reloads it when the
    underlying config file path or modification time changes. Use
    `reload_app_config()` to force a reload, or `reset_app_config()` to clear
    the cache.
    """
    global _app_config, _app_config_path, _app_config_mtime

    if _app_config is not None and _app_config_is_custom:
        return _app_config

    resolved_path = AppConfig.resolve_config_path()
    current_mtime = _get_config_mtime(resolved_path)

    should_reload = (
        _app_config is None
        or _app_config_path != resolved_path
        or _app_config_mtime != current_mtime
    )
    if should_reload:
        if (
            _app_config_path == resolved_path
            and _app_config_mtime is not None
            and current_mtime is not None
            and _app_config_mtime != current_mtime
        ):
            logger.info(
                "Config file has been modified (mtime: %s -> %s), reloading AppConfig",
                _app_config_mtime,
                current_mtime,
            )
        _load_and_cache_app_config(str(resolved_path))
    return _app_config
```

**File:** backend/packages/harness/deerflow/mcp/cache.py (L31-53)
```python
def _is_cache_stale() -> bool:
    """Check if the cache is stale due to config file changes.

    Returns:
        True if the cache should be invalidated, False otherwise.
    """
    global _config_mtime

    if not _cache_initialized:
        return False  # Not initialized yet, not stale

    current_mtime = _get_config_mtime()

    # If we couldn't get mtime before or now, assume not stale
    if _config_mtime is None or current_mtime is None:
        return False

    # If the config file has been modified since we cached, it's stale
    if current_mtime > _config_mtime:
        logger.info(f"MCP config file has been modified (mtime: {_config_mtime} -> {current_mtime}), cache is stale")
        return True

    return False
```

**File:** backend/packages/harness/deerflow/mcp/cache.py (L82-102)
```python
def get_cached_mcp_tools() -> list[BaseTool]:
    """Get cached MCP tools with lazy initialization.

    If tools are not initialized, automatically initializes them.
    This ensures MCP tools work in both FastAPI and LangGraph Studio contexts.

    Also checks if the config file has been modified since last initialization,
    and re-initializes if needed. This ensures that changes made through the
    Gateway API (which runs in a separate process) are reflected in the
    LangGraph Server.

    Returns:
        List of cached MCP tools.
    """
    global _cache_initialized

    # Check if cache is stale due to config file changes
    if _is_cache_stale():
        logger.info("MCP cache is stale, resetting for re-initialization...")
        reset_mcp_tools_cache()

```

**File:** backend/packages/harness/deerflow/skills/loader.py (L77-89)
```python
    # NOTE: We use ExtensionsConfig.from_file() instead of get_extensions_config()
    # to always read the latest configuration from disk. This ensures that changes
    # made through the Gateway API (which runs in a separate process) are immediately
    # reflected in the LangGraph Server when loading skills.
    try:
        from deerflow.config.extensions_config import ExtensionsConfig

        extensions_config = ExtensionsConfig.from_file()
        for skill in skills:
            skill.enabled = extensions_config.is_skill_enabled(skill.name, skill.category)
    except Exception as e:
        # If config loading fails, default to all enabled
        print(f"Warning: Failed to load extensions config: {e}")
```

**File:** backend/packages/harness/deerflow/config/agents_config.py (L72-89)
```python
def load_agent_soul(agent_name: str | None) -> str | None:
    """Read the SOUL.md file for a custom agent, if it exists.

    SOUL.md defines the agent's personality, values, and behavioral guardrails.
    It is injected into the lead agent's system prompt as additional context.

    Args:
        agent_name: The name of the agent or None for the default agent.

    Returns:
        The SOUL.md content as a string, or None if the file does not exist.
    """
    agent_dir = get_paths().agent_dir(agent_name) if agent_name else get_paths().base_dir
    soul_path = agent_dir / SOUL_FILENAME
    if not soul_path.exists():
        return None
    content = soul_path.read_text(encoding="utf-8").strip()
    return content or None
```

**File:** frontend/src/env.js (L27-49)
```javascript
  client: {
    NEXT_PUBLIC_BACKEND_BASE_URL: z.string().optional(),
    NEXT_PUBLIC_LANGGRAPH_BASE_URL: z.string().optional(),
    NEXT_PUBLIC_STATIC_WEBSITE_ONLY: z.string().optional(),
  },

  /**
   * You can't destruct `process.env` as a regular object in the Next.js edge runtimes (e.g.
   * middlewares) or client-side so we need to destruct manually.
   */
  runtimeEnv: {
    BETTER_AUTH_SECRET: process.env.BETTER_AUTH_SECRET,
    BETTER_AUTH_GITHUB_CLIENT_ID: process.env.BETTER_AUTH_GITHUB_CLIENT_ID,
    BETTER_AUTH_GITHUB_CLIENT_SECRET:
      process.env.BETTER_AUTH_GITHUB_CLIENT_SECRET,
    NODE_ENV: process.env.NODE_ENV,

    NEXT_PUBLIC_BACKEND_BASE_URL: process.env.NEXT_PUBLIC_BACKEND_BASE_URL,
    NEXT_PUBLIC_LANGGRAPH_BASE_URL: process.env.NEXT_PUBLIC_LANGGRAPH_BASE_URL,
    NEXT_PUBLIC_STATIC_WEBSITE_ONLY:
      process.env.NEXT_PUBLIC_STATIC_WEBSITE_ONLY,
    GITHUB_OAUTH_TOKEN: process.env.GITHUB_OAUTH_TOKEN,
  },
```

**File:** Makefile (L44-45)
```text
	@echo "Installing backend dependencies..."
	@cd backend && uv sync
```

**File:** Makefile (L46-47)
```text
	@echo "Installing frontend dependencies..."
	@cd frontend && pnpm install
```

**File:** Makefile (L91-97)
```text
# Start all services in development mode (with hot-reloading)
dev:
	@./scripts/serve.sh --dev

# Start all services in production mode (with optimizations)
start:
	@./scripts/serve.sh --prod
```

**File:** docker/nginx/nginx.local.conf (L1-50)
```text
events {
    worker_connections 1024;
}
pid logs/nginx.pid;
http {
    # Basic settings
    sendfile on;
    tcp_nopush on;
    tcp_nodelay on;
    keepalive_timeout 65;
    types_hash_max_size 2048;

    # Logging
    access_log logs/nginx-access.log;
    error_log logs/nginx-error.log;

    # Upstream servers (using 127.0.0.1 for local development)
    upstream gateway {
        server 127.0.0.1:8001;
    }

    upstream langgraph {
        server 127.0.0.1:2024;
    }

    upstream frontend {
        server 127.0.0.1:3000;
    }

    server {
        listen 2026;
        listen [::]:2026;
        server_name _;

        # Hide CORS headers from upstream to prevent duplicates
        proxy_hide_header 'Access-Control-Allow-Origin';
        proxy_hide_header 'Access-Control-Allow-Methods';
        proxy_hide_header 'Access-Control-Allow-Headers';
        proxy_hide_header 'Access-Control-Allow-Credentials';

        # CORS headers for all responses (nginx handles CORS centrally)
        add_header 'Access-Control-Allow-Origin' '*' always;
        add_header 'Access-Control-Allow-Methods' 'GET, POST, PUT, DELETE, PATCH, OPTIONS' always;
        add_header 'Access-Control-Allow-Headers' '*' always;

        # Handle OPTIONS requests (CORS preflight)
        if ($request_method = 'OPTIONS') {
            return 204;
        }

```

**File:** backend/app/gateway/config.py (L14-27)
```python
_gateway_config: GatewayConfig | None = None


def get_gateway_config() -> GatewayConfig:
    """Get gateway config, loading from environment if available."""
    global _gateway_config
    if _gateway_config is None:
        cors_origins_str = os.getenv("CORS_ORIGINS", "http://localhost:3000")
        _gateway_config = GatewayConfig(
            host=os.getenv("GATEWAY_HOST", "0.0.0.0"),
            port=int(os.getenv("GATEWAY_PORT", "8001")),
            cors_origins=cors_origins_str.split(","),
        )
    return _gateway_config
```
