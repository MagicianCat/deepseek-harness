import { describe, expect, it } from 'vitest'
import { normalizeToolArguments } from '../src/tool-arguments.js'

describe('normalizeToolArguments', () => {
  it('keeps an object argument', () => {
    const value = { query: 'requirements', developmentStage: 'REQUIREMENT' }
    expect(normalizeToolArguments(value)).toBe(value)
  })

  it('parses a JSON string argument from the SDK event', () => {
    expect(normalizeToolArguments('{"query":"requirements","developmentStage":"REQUIREMENT"}'))
      .toEqual({ query: 'requirements', developmentStage: 'REQUIREMENT' })
  })

  it.each([
    '',
    'not-json',
    'null',
    '[]',
    '"text"',
    '42',
  ])('returns an empty object for invalid or non-object input: %s', (value) => {
    expect(normalizeToolArguments(value)).toEqual({})
  })

  it('returns an empty object for non-string primitive input', () => {
    expect(normalizeToolArguments(undefined)).toEqual({})
    expect(normalizeToolArguments(null)).toEqual({})
    expect(normalizeToolArguments(false)).toEqual({})
  })
})
