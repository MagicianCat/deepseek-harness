# Skill Platform DSH Runtime

[English](README.md) | 中文

这是一个内部 HTTP/SSE 适配器，每个 Skill 推荐 Run 都会启动一个隔离的 `sdk-minimal` DSH 子进程。浏览器不会直接访问它。

```bash
DSH_SERVICE_TOKEN='local-service-token' \
DEEPSEEK_API_KEY='from-secret-store' \
SKILL_PLATFORM_MCP_URL='http://127.0.0.1:8090/internal/mcp' \
DSH_MAX_TOKENS='16384' \
DSH_REASONING_EFFORT='high' \
DSH_HISTORY_MAX_MESSAGES='8' \
DSH_HISTORY_MAX_CHARS='16000' \
pnpm --filter @company/skill-platform-dsh-runtime dev
```

默认端口为 `3090`。SDK patch 会移除 shell 和编辑器工具，模型只能使用通过认证的 Skill Platform MCP 工具。

飞书文档访问由 Skill Platform MCP 端点代理。Runtime 不接收或保存飞书用户 Token；后端执行按用户授权的只读飞书查询，并只暴露清洗后的结果。

Runtime 只向 stderr 写入少量生命周期日志（`run.started`、`run.completed`、`run.failed` 或 `run.cancelled`），其中不包含凭据。Provider 或模型错误会作为 `runtime.failed` 暴露；模型返回空响应时会进行一次有界恢复，仍为空则报告 `EMPTY_RESPONSE_AFTER_RETRY`。`DSH_MAX_TOKENS` 默认值为 `16384`，`DSH_RUN_TIMEOUT_MS` 默认为 240 秒，`DSH_NO_PROGRESS_TIMEOUT_MS` 默认为 90 秒。

平台 MCP 协议将 Skill 搜索和结构化推荐限制为 20 项。用户要求全部匹配 Skill 时，Runtime 必须说明结果是否被截断，不能无限翻页。

Runtime 会先通过平台 MCP 检索用户有权访问的 Wiki 知识，再进行宽泛 Wiki 或 Skill 搜索。知识结果包含有界片段和关联 Skill 摘要；Wiki 全文和 Skill 文件只作为降级读取。

Runtime 还强制执行证据规则：每轮先识别当前团队；研发阶段请求必须查询携带阶段的知识结果，或使用旧版最佳实践 Wiki 与阶段 Skill 降级链路；保险问题必须读取飞书来源文档并包含 `来源：` 引用。

对话历史默认只保留最近八条和 16000 字符。Skill Advisor 的推理强度默认为 `high`（支持 `off`、`low`、`high` 和 `max`）；历史限制和推理强度都可以通过上方环境变量配置。

飞书 MCP 故障会被视为可恢复工具结果。平台后端会对短暂上游故障执行有界重试，并向模型返回经过清洗的结构化错误；Runtime 在报告 Run 失败前会执行一次有界恢复。
