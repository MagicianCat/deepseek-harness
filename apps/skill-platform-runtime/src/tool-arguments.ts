export type ToolArguments = Record<string, unknown>

/**
 * Normalize MCP tool arguments at the runtime event boundary.
 * SDK adapters may expose arguments as either an object or a JSON string.
 */
export function normalizeToolArguments(value: unknown): ToolArguments {
  if (isRecord(value)) return value
  if (typeof value !== 'string' || value.trim() === '') return {}

  try {
    const parsed: unknown = JSON.parse(value)
    return isRecord(parsed) ? parsed : {}
  } catch {
    return {}
  }
}

function isRecord(value: unknown): value is ToolArguments {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}
