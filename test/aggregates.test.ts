import { avg, count, eq, max, min, sql, sum } from 'drizzle-orm';
import { beforeAll, describe, expect, it } from 'vitest';
import type { SnowflakeDatabase } from '../src/index.ts';
import { getDb, products } from './setup.ts';

describe('aggregate functions', () => {
  let db: SnowflakeDatabase;

  beforeAll(async () => {
    db = await getDb();
  });

  describe('basic aggregates via ORM', () => {
    it('should compute count()', async () => {
      const result = await db.select({ cnt: count() }).from(products) as any;
      expect(result).toHaveLength(1);
      expect(Number(result[0]!.cnt)).toBe(5);
    });

    it('should compute avg()', async () => {
      const result = await db.select({ avgPrice: avg(products.price) }).from(products) as any;
      expect(result).toHaveLength(1);
      expect(Number(result[0]!.avgPrice)).toBeGreaterThan(0);
    });

    it('should compute sum()', async () => {
      const result = await db.select({ total: sum(products.quantity) }).from(products) as any;
      expect(result).toHaveLength(1);
      expect(Number(result[0]!.total)).toBe(43);
    });

    it('should compute min() and max()', async () => {
      const result = await db.select({
        minPrice: min(products.price),
        maxPrice: max(products.price),
      }).from(products) as any;
      expect(result).toHaveLength(1);
      expect(Number(result[0]!.minPrice)).toBe(4.99);
      expect(Number(result[0]!.maxPrice)).toBe(99.99);
    });

    it('should aggregate with groupBy', async () => {
      const result = await db
        .select({
          category: products.category,
          cnt: count(),
          avgPrice: avg(products.price),
        })
        .from(products)
        .groupBy(products.category)
        .orderBy(products.category) as any;
      expect(result).toHaveLength(3);
    });
  });

  describe('COALESCE wrapping', () => {
    it('should handle COALESCE(AVG(...), 0)', async () => {
      const result = await db
        .select({
          category: products.category,
          avgPrice: sql<number>`COALESCE(AVG(${products.price}), 0)`.as('avg_price'),
        })
        .from(products)
        .groupBy(products.category)
        .orderBy(products.category) as any;
      expect(result).toHaveLength(3);
      for (const row of result) {
        expect(Number(row.avgPrice)).toBeGreaterThanOrEqual(0);
      }
    });

    it('should handle COALESCE(SUM(...), 0)', async () => {
      const result = await db
        .select({
          category: products.category,
          totalQty: sql<number>`COALESCE(SUM(${products.quantity}), 0)`.as('total_qty'),
        })
        .from(products)
        .groupBy(products.category) as any;
      expect(result).toHaveLength(3);
    });

    it('should return 0 for COALESCE with impossible filter', async () => {
      const result = await db
        .select({
          total: sql<number>`COALESCE(SUM(${products.price}), 0)`.as('total'),
        })
        .from(products)
        .where(eq(products.category, 'nonexistent')) as any;
      expect(result).toHaveLength(1);
      expect(Number(result[0]!.total)).toBe(0);
    });
  });

  describe('CASE WHEN in aggregates', () => {
    it('should count with CASE WHEN', async () => {
      const result = await db
        .select({
          activeCount: sql<number>`COUNT(CASE WHEN ${products.active} = true THEN 1 END)`.as('active_count'),
          inactiveCount: sql<number>`COUNT(CASE WHEN ${products.active} = false THEN 1 END)`.as('inactive_count'),
        })
        .from(products) as any;
      expect(result).toHaveLength(1);
      expect(Number(result[0]!.activeCount)).toBe(4);
      expect(Number(result[0]!.inactiveCount)).toBe(1);
    });

    it('should sum with CASE WHEN per category', async () => {
      const result = await db
        .select({
          hwTotal: sql<number>`SUM(CASE WHEN ${products.category} = 'hardware' THEN ${products.price} ELSE 0 END)`.as('hw_total'),
          accTotal: sql<number>`SUM(CASE WHEN ${products.category} = 'accessories' THEN ${products.price} ELSE 0 END)`.as('acc_total'),
        })
        .from(products) as any;
      expect(result).toHaveLength(1);
      expect(Number(result[0]!.hwTotal)).toBeGreaterThan(0);
      expect(Number(result[0]!.accTotal)).toBeGreaterThan(0);
    });

    it('should use CASE WHEN with GROUP BY', async () => {
      const result = await db
        .select({
          category: products.category,
          total: count(),
          activeCount: sql<number>`COUNT(CASE WHEN ${products.active} = true THEN 1 END)`.as('active_count'),
        })
        .from(products)
        .groupBy(products.category)
        .orderBy(products.category) as any;
      expect(result).toHaveLength(3);
    });
  });

  describe('statistical functions', () => {
    it('should support STDDEV_POP', async () => {
      const result = await db
        .select({ stddev: sql<number>`STDDEV_POP(${products.price})`.as('stddev') })
        .from(products) as any;
      expect(result).toHaveLength(1);
      expect(Number(result[0]!.stddev)).toBeGreaterThan(0);
    });

    it('should support STDDEV_SAMP', async () => {
      const result = await db
        .select({ stddev: sql<number>`STDDEV_SAMP(${products.price})`.as('stddev') })
        .from(products) as any;
      expect(result).toHaveLength(1);
      expect(Number(result[0]!.stddev)).toBeGreaterThan(0);
    });

    it('should support VAR_POP', async () => {
      const result = await db
        .select({ variance: sql<number>`VAR_POP(${products.price})`.as('variance') })
        .from(products) as any;
      expect(result).toHaveLength(1);
      expect(Number(result[0]!.variance)).toBeGreaterThan(0);
    });

    it('should support VAR_SAMP', async () => {
      const result = await db
        .select({ variance: sql<number>`VAR_SAMP(${products.price})`.as('variance') })
        .from(products) as any;
      expect(result).toHaveLength(1);
      expect(Number(result[0]!.variance)).toBeGreaterThan(0);
    });
  });
});
