import { eq, gt, gte, lt } from 'drizzle-orm';
import { beforeAll, describe, expect, it } from 'vitest';
import type { SnowflakeDatabase } from '../src/index.ts';
import { getDb, products } from './setup.ts';

describe('type coercion', () => {
  let db: SnowflakeDatabase;

  beforeAll(async () => {
    db = await getDb();
  });

  it('should compare integer column with gt()', async () => {
    const result = await db.select().from(products).where(gt(products.quantity, 5)) as any;
    expect(result).toHaveLength(2);
    for (const row of result) {
      expect(row.quantity).toBeGreaterThan(5);
    }
  });

  it('should compare integer column with eq()', async () => {
    const result = await db.select().from(products).where(eq(products.quantity, 0)) as any;
    expect(result).toHaveLength(1);
    expect(result[0]!.name).toBe('Doohickey');
  });

  it('should compare double column with gt()', async () => {
    const result = await db.select().from(products).where(gt(products.price, 20.0)) as any;
    expect(result).toHaveLength(2);
    for (const row of result) {
      expect(row.price).toBeGreaterThan(20.0);
    }
  });

  it('should compare double column with lt()', async () => {
    const result = await db.select().from(products).where(lt(products.price, 10.0)) as any;
    expect(result).toHaveLength(2);
    for (const row of result) {
      expect(row.price).toBeLessThan(10.0);
    }
  });

  it('should compare integer column with gte()', async () => {
    const result = await db.select().from(products).where(gte(products.quantity, 10)) as any;
    expect(result).toHaveLength(2);
  });

  it('should handle integer 0 comparison', async () => {
    const result = await db.select().from(products).where(gt(products.quantity, 0)) as any;
    expect(result).toHaveLength(4);
  });
});
