import { sql } from 'drizzle-orm';
import { describe, expect, it } from 'vitest';
import { SnowflakeDialect } from '../src/dialect.ts';

describe('SnowflakeDialect', () => {
  const dialect = new SnowflakeDialect();

  describe('sqlToQuery - parameter style', () => {
    it('should use ? for params', () => {
      const query = sql`SELECT * FROM users WHERE id = ${1}`;
      const result = dialect.sqlToQuery(query);
      expect(result.sql).not.toContain('$1');
      expect(result.sql).toContain('?');
      expect(result.params).toEqual([1]);
    });

    it('should use ? for multiple params', () => {
      const query = sql`SELECT * FROM users WHERE id = ${1} AND name = ${'alice'}`;
      const result = dialect.sqlToQuery(query);
      expect(result.sql).not.toMatch(/\$\d+/);
      const questionMarks = result.sql.match(/\?/g);
      expect(questionMarks).toHaveLength(2);
      expect(result.params).toEqual([1, 'alice']);
    });

    it('should handle query with no params', () => {
      const query = sql`SELECT 1`;
      const result = dialect.sqlToQuery(query);
      expect(result.sql).toBe('SELECT 1');
      expect(result.params).toEqual([]);
    });

    it('should handle many params', () => {
      const query = sql`INSERT INTO t VALUES (${1}, ${2}, ${3}, ${4}, ${5}, ${6}, ${7}, ${8}, ${9}, ${10}, ${11})`;
      const result = dialect.sqlToQuery(query);
      expect(result.sql).not.toMatch(/\$\d+/);
      const questionMarks = result.sql.match(/\?/g);
      expect(questionMarks).toHaveLength(11);
    });
  });

  describe('escapeName', () => {
    it('should return quoted names by default', () => {
      expect(dialect.escapeName('table_name')).toBe('"table_name"');
    });
  });

  describe('escapeParam', () => {
    it('should use ? for all params', () => {
      expect(dialect.escapeParam(0)).toBe('?');
      expect(dialect.escapeParam(1)).toBe('?');
      expect(dialect.escapeParam(10)).toBe('?');
    });
  });
});
