import { describe, it, expect } from 'vitest';
import { sql } from 'drizzle-orm';
import { SnowflakeDialect } from '../src/dialect.ts';

describe('SnowflakeDialect', () => {
  const dialect = new SnowflakeDialect();

  describe('sqlToQuery - parameter rewriting', () => {
    it('should rewrite $1 to ?', () => {
      const query = sql`SELECT * FROM users WHERE id = ${1}`;
      const result = dialect.sqlToQuery(query);
      expect(result.sql).not.toContain('$1');
      expect(result.sql).toContain('?');
      expect(result.params).toEqual([1]);
    });

    it('should rewrite multiple $N params to ?', () => {
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

  describe('areSavepointsUnsupported', () => {
    it('should return true', () => {
      expect(dialect.areSavepointsUnsupported()).toBe(true);
    });
  });
});
