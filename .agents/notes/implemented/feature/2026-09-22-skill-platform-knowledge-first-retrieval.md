# Agent Note: Skill Platform knowledge-first retrieval

Status: implemented

English | [中文](2026-09-22-skill-platform-knowledge-first-retrieval.zh.md)

## Problem

The Skill Advisor had to locate relevant Wiki guidance through repeated title searches, full-document reads, broad Skill searches, and one detail read per candidate. The number of model-tool round trips and the prompt size grew with the knowledge base even when the platform already knew which Wiki documents and Skills were related.

## Decision

The Skill Advisor calls the Skill Platform MCP `search_knowledge` tool after current-user discovery. The backend owns semantic retrieval, authorization filtering, Wiki snippet selection, and linked Skill summaries. A sufficient knowledge result is authoritative discovery evidence, so the runtime does not repeat broad Wiki or Skill searches and reads full Wiki or Skill files only when the user asks for details that the snippets do not contain.

Development-stage evidence accepts one stage-qualified knowledge search. The existing best-practice Wiki plus stage-filtered Skill search remains a valid fallback when retrieval is unavailable. Insurance questions retain their separate Feishu read-and-citation requirement.

The runtime bounds untrusted conversation history to eight messages and 16,000 characters by default. `DSH_HISTORY_MAX_MESSAGES`, `DSH_HISTORY_MAX_CHARS`, and `DSH_REASONING_EFFORT` expose deployment tuning; Skill Advisor reasoning defaults to `medium`.

## Alternatives considered

**Keep title search and full reads as the default path.** This preserves the old tool policy but retains the latency and context growth that semantic retrieval is intended to remove.

**Connect DSH directly to Qdrant.** This shortens the network path but gives the model-facing runtime access to infrastructure credentials and moves team authorization out of the backend that owns membership and Wiki visibility.

**Remove legacy tools after adding knowledge search.** This makes retrieval failure fatal and prevents precise title lookup or full-detail answers, so the existing tools remain bounded fallbacks.

## Consequences

- Typical recommendations use current-user discovery, one knowledge search, and one structured recommendation submission.
- The backend can change vector storage, embedding, or ranking without changing the DSH protocol beyond `search_knowledge` results.
- Retrieval outages increase tool calls because the runtime falls back, but they do not remove the ability to answer.
- Bounded history reduces latency and prevents indefinite prompt growth, at the cost of dropping older verbatim turns.
