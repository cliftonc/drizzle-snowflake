import { and, eq, gt } from 'drizzle-orm';
import { beforeAll, describe, expect, it } from 'vitest';
import type { SnowflakeDatabase } from '../src/index.ts';
import { getDb, items, users } from './setup.ts';

describe('parameter binding', () => {
  let db: SnowflakeDatabase;

  beforeAll(async () => {
    db = await getDb();
  });

  it('should bind string params', async () => {
    const result = await db.select().from(users).where(eq(users.name, 'Alice')) as any;
    expect(result).toHaveLength(1);
    expect(result[0]!.name).toBe('Alice');
  });

  it('should bind integer params', async () => {
    const result = await db.select().from(users).where(eq(users.id, 1)) as any;
    expect(result).toHaveLength(1);
    expect(result[0]!.name).toBe('Alice');
  });

  it('should bind boolean params', async () => {
    const result = await db.select().from(users).where(eq(users.active, true)) as any;
    expect(result).toHaveLength(3);
    for (const row of result) {
      expect(row.active).toBe(true);
    }
  });

  it('should bind real/float params', async () => {
    const result = await db.select().from(users).where(eq(users.score, 9.5)) as any;
    expect(result).toHaveLength(1);
    expect(result[0]!.name).toBe('Alice');
  });

  it('should bind multiple params in one query', async () => {
    const result = await db
      .select()
      .from(users)
      .where(and(eq(users.active, true), gt(users.score, 7.0))) as any;
    expect(result).toHaveLength(2);
  });

  describe('string escaping', () => {
    it('should handle single quotes in string params', async () => {
      const result = await db.select().from(users).where(eq(users.name, "O'Brien")) as any;
      expect(result).toHaveLength(1);
      expect(result[0]!.name).toBe("O'Brien");
    });

    it('should handle strings with multiple single quotes', async () => {
      // Query that returns no rows but exercises the escaping
      const result = await db.select().from(users).where(eq(users.name, "it's a 'test'")) as any;
      expect(result).toHaveLength(0);
    });

    it('should not be vulnerable to SQL injection via string params', async () => {
      const malicious = "'; DROP TABLE drizzle_test_users; --";
      const result = await db.select().from(users).where(eq(users.name, malicious)) as any;
      expect(result).toHaveLength(0);
      // Table should still exist
      const check = await db.select().from(users) as any;
      expect(check).toHaveLength(4);
    });

    it('should handle VARIANT values with embedded quotes', async () => {
      const result = await db.select().from(items).where(eq(items.name, 'Quoted Item')) as any;
      expect(result).toHaveLength(1);
      const meta = result[0]!.metadata as { note: string };
      expect(meta.note).toBe("it's a test");
    });
  });
});
