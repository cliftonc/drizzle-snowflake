import { eq, like, sql } from 'drizzle-orm';
import { beforeAll, describe, expect, it } from 'vitest';
import type { SnowflakeDatabase } from '../src/index.ts';
import { getDb, products, users } from './setup.ts';

describe('LIKE and LOWER patterns', () => {
  let db: SnowflakeDatabase;

  beforeAll(async () => {
    db = await getDb();
  });

  it('should handle basic LIKE', async () => {
    const result = await db.select().from(users).where(like(users.name, 'Ali%')) as any;
    expect(result).toHaveLength(1);
    expect(result[0]!.name).toBe('Alice');
  });

  it('should handle LIKE with underscore wildcard', async () => {
    const result = await db.select().from(users).where(like(users.name, 'Bo_')) as any;
    expect(result).toHaveLength(1);
    expect(result[0]!.name).toBe('Bob');
  });

  it('should handle LIKE with percent in middle', async () => {
    const result = await db.select().from(users).where(like(users.name, 'A%e')) as any;
    expect(result).toHaveLength(1);
    expect(result[0]!.name).toBe('Alice');
  });

  it('should handle LOWER() in where clause via sql expression', async () => {
    const result = await db
      .select()
      .from(users)
      .where(eq(sql`LOWER(${users.name})`, 'alice')) as any;
    expect(result).toHaveLength(1);
    expect(result[0]!.name).toBe('Alice');
  });

  it('should handle LOWER + LIKE via sql expression', async () => {
    const pattern = '%alice%';
    const result = await db
      .select()
      .from(users)
      .where(sql`LOWER(${users.name}) LIKE ${pattern}`) as any;
    expect(result).toHaveLength(1);
  });

  it('should handle LOWER + LIKE with single quote in param', async () => {
    const pattern = "%o'b%";
    const result = await db
      .select()
      .from(users)
      .where(sql`LOWER(${users.name}) LIKE ${pattern}`) as any;
    expect(result).toHaveLength(1);
  });

  it('should handle case-insensitive search across categories', async () => {
    const pattern = '%hard%';
    const result = await db
      .select()
      .from(products)
      .where(sql`LOWER(${products.category}) LIKE ${pattern}`) as any;
    expect(result).toHaveLength(2);
  });

  it('should handle LOWER in select expression', async () => {
    const result = await db
      .select({
        lowerName: sql<string>`LOWER(${users.name})`.as('lower_name'),
      })
      .from(users)
      .where(eq(users.id, 1)) as any;
    expect(result).toHaveLength(1);
    expect(result[0]!.lowerName).toBe('alice');
  });
});
