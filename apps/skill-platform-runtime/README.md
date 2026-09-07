# Skill Platform DSH Runtime

Internal HTTP/SSE adapter that runs one isolated `sdk-minimal` DSH child per
Skill recommendation run. It is not browser-facing.

```bash
DSH_SERVICE_TOKEN='local-service-token' \
DEEPSEEK_API_KEY='from-secret-store' \
SKILL_PLATFORM_MCP_URL='http://127.0.0.1:8090/internal/mcp' \
pnpm --filter @company/skill-platform-dsh-runtime dev
```

The default port is `3090`. The SDK patch removes shell and editor tools; only
the authenticated Skill Platform MCP tools remain available to the model.

The runtime writes a small, stderr-only lifecycle log (`run.started`,
`run.completed`, `run.failed`, or `run.cancelled`) containing no credentials.
Provider/model failures are surfaced as `runtime.failed`; an empty model
response is rejected with `EMPTY_RESPONSE` instead of being reported as
successful.
