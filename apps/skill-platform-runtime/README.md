# Skill Platform DSH Runtime

Internal HTTP/SSE adapter that runs one isolated `sdk-minimal` DSH child per
Skill recommendation run. It is not browser-facing.

```bash
DSH_SERVICE_TOKEN='local-service-token' \
DEEPSEEK_API_KEY='from-secret-store' \
SKILL_PLATFORM_MCP_URL='http://127.0.0.1:8090/internal/mcp' \
DSH_MAX_TOKENS='16384' \
pnpm --filter @company/skill-platform-dsh-runtime dev
```

The default port is `3090`. The SDK patch removes shell and editor tools; only
the authenticated Skill Platform MCP tools remain available to the model.

Feishu document access is mediated by the Skill Platform MCP endpoint. This
runtime never receives or stores a Feishu user token; the backend performs the
per-user, read-only Feishu lookup and exposes only sanitized results.

The runtime writes a small, stderr-only lifecycle log (`run.started`,
`run.completed`, `run.failed`, or `run.cancelled`) containing no credentials.
Provider/model failures are surfaced as `runtime.failed`; an empty model
response gets one bounded recovery turn and is reported as
`EMPTY_RESPONSE_AFTER_RETRY` if it remains empty. `DSH_MAX_TOKENS` defaults to
`16384`, `DSH_RUN_TIMEOUT_MS` to 240 seconds, and
`DSH_NO_PROGRESS_TIMEOUT_MS` to 90 seconds.

Skill search and structured recommendation are bounded to 20 items by the
platform MCP contract. Requests for all matching skills must report when the
result set is truncated instead of paging indefinitely.

The runtime also enforces evidence gates: every turn starts with current-team
discovery, development-stage requests must consult the `研发全流程最佳实践`
Wiki and stage-filtered skills, and insurance answers must read Feishu source
documents and include a `来源：` citation.

Feishu MCP failures are treated as recoverable tool results. The platform
backend performs bounded retries for transient upstream failures and returns
sanitized structured errors for the model; the runtime gets one bounded
recovery turn before reporting a run failure.
