# Agent Note: Skill Platform knowledge-first retrieval

Status: implemented

[English](2026-09-22-skill-platform-knowledge-first-retrieval.md) | 中文

## Problem

Skill Advisor 必须通过多次标题搜索、Wiki 全文读取、宽泛的 Skill 搜索以及逐个候选详情读取来定位相关指导。即使平台已经维护 Wiki 与 Skill 的关联关系，模型与工具之间的往返次数和 Prompt 大小仍会随知识库增长。

## Decision

Skill Advisor 在识别当前用户后调用 Skill Platform MCP 的 `search_knowledge` 工具。后端负责语义检索、权限过滤、Wiki 片段选择和关联 Skill 摘要。知识检索结果足够时即作为可信发现依据，Runtime 不再重复宽泛搜索 Wiki 或 Skill；只有用户要求的细节不在片段中时才读取 Wiki 或 Skill 全文。

研发阶段证据允许一次携带准确阶段的知识检索。检索不可用时，原有的最佳实践 Wiki 搜索和阶段 Skill 搜索仍是有效降级路径。保险问题继续遵守独立的飞书读取与引用要求。

Runtime 默认仅保留最近八条、最多 16000 字符的非可信对话历史。部署可通过 `DSH_HISTORY_MAX_MESSAGES`、`DSH_HISTORY_MAX_CHARS` 和 `DSH_REASONING_EFFORT` 调整；Skill Advisor 的推理强度默认为 `medium`。

## Alternatives considered

**继续默认使用标题搜索和全文读取。** 这能维持旧 Tool Policy，但无法消除语义检索要解决的延迟和上下文增长。

**让 DSH 直接连接 Qdrant。** 这会缩短网络路径，但会把基础设施凭据交给面向模型的 Runtime，并把团队权限判断移出维护成员关系和 Wiki 可见性的后端。

**加入知识检索后删除旧工具。** 这会让检索故障直接导致失败，也无法支持精确标题查找和完整细节回答，因此旧工具继续作为有界降级路径。

## Consequences

- 典型推荐只需要识别当前用户、一次知识检索和一次结构化推荐提交。
- 后端可以替换向量存储、Embedding 或排序实现，而无需改变 DSH 中除 `search_knowledge` 结果之外的协议。
- 检索故障时工具调用数会上升，但仍可通过旧链路回答。
- 历史截断降低延迟并阻止 Prompt 无限增长，代价是更早的逐字对话会被丢弃。
