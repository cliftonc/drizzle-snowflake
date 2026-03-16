import { describe, it, expect } from 'vitest';
import { prepareParams, isPool } from '../src/client.ts';

describe('prepareParams', () => {
  it('should convert undefined to null', () => {
    expect(prepareParams([undefined])).toEqual([null]);
  });

  it('should convert Date to ISO string', () => {
    const date = new Date('2024-01-15T12:30:00Z');
    const result = prepareParams([date]);
    expect(result[0]).toBe('2024-01-15T12:30:00.000Z');
  });

  it('should convert bigint to string', () => {
    const result = prepareParams([BigInt('9007199254740993')]);
    expect(result[0]).toBe('9007199254740993');
  });

  it('should pass through strings', () => {
    expect(prepareParams(['hello'])).toEqual(['hello']);
  });

  it('should pass through numbers', () => {
    expect(prepareParams([42, 3.14])).toEqual([42, 3.14]);
  });

  it('should pass through null', () => {
    expect(prepareParams([null])).toEqual([null]);
  });

  it('should pass through booleans', () => {
    expect(prepareParams([true, false])).toEqual([true, false]);
  });

  it('should handle mixed param types', () => {
    const date = new Date('2024-06-01T00:00:00Z');
    const result = prepareParams([
      1,
      'text',
      null,
      undefined,
      date,
      BigInt(100),
      true,
    ]);
    expect(result).toEqual([
      1,
      'text',
      null,
      null,
      '2024-06-01T00:00:00.000Z',
      '100',
      true,
    ]);
  });

  it('should handle empty array', () => {
    expect(prepareParams([])).toEqual([]);
  });
});

describe('isPool', () => {
  it('should return true for objects with acquire method', () => {
    const pool = {
      acquire: async () => ({} as any),
      release: async () => {},
    };
    expect(isPool(pool)).toBe(true);
  });

  it('should return false for plain objects', () => {
    const conn = { execute: () => {}, connect: () => {} } as any;
    expect(isPool(conn)).toBe(false);
  });
});
