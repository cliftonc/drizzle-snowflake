import { avg, count, eq, sql, sum } from 'drizzle-orm';
import { beforeAll, describe, expect, it } from 'vitest';
import type { SnowflakeDatabase } from '../src/index.ts';
import { getDb, products } from './setup.ts';

describe('CTE patterns', () => {
  let db: SnowflakeDatabase;

  beforeAll(async () => {
    db = await getDb();
  });

  it('should handle a simple CTE with $with', async () => {
    const activeProducts = db.$with('active_products').as(
      db.select({ id: products.id, name: products.name, price: products.price })
        .from(products)
        .where(eq(products.active, true)),
    );

    const result = await db.with(activeProducts)
      .select()
      .from(activeProducts)
      .orderBy(sql`${(activeProducts as any).price} DESC`) as any;

    expect(result).toHaveLength(4);
    for (const row of result) {
      expect((row as any).price).toBeDefined();
    }
  });

  it('should handle CTE with aggregation', async () => {
    const categoryStats = db.$with('category_stats').as(
      db.select({
        category: products.category,
        cnt: count().as('cnt'),
        avgPrice: avg(products.price).as('avg_price'),
      })
        .from(products)
        .groupBy(products.category),
    );

    const result = await db.with(categoryStats)
      .select()
      .from(categoryStats)
      .orderBy((categoryStats as any).category) as any;

    expect(result).toHaveLength(3);
    for (const row of result) {
      expect(Number(row.cnt)).toBeGreaterThan(0);
    }
  });

  it('should handle CTE with LEFT JOIN back to source table', async () => {
    const categoryStats = db.$with('category_stats').as(
      db.select({
        category: products.category,
        cnt: count().as('cnt'),
        avgPrice: avg(products.price).as('avg_price'),
      })
        .from(products)
        .groupBy(products.category),
    );

    const result = await db.with(categoryStats)
      .select({
        name: products.name,
        price: products.price,
        categoryCnt: (categoryStats as any).cnt,
        categoryAvg: (categoryStats as any).avgPrice,
      })
      .from(products)
      .leftJoin(categoryStats, eq(products.category, (categoryStats as any).category))
      .orderBy(products.id) as any;

    expect(result).toHaveLength(5);
    for (const row of result) {
      expect(row.categoryCnt).toBeDefined();
      expect(Number(row.categoryCnt)).toBeGreaterThan(0);
    }
  });

  it('should handle multiple CTEs referencing earlier CTE', async () => {
    const base = db.$with('base').as(
      db.select({
        category: products.category,
        price: products.price,
      })
        .from(products)
        .where(eq(products.active, true)),
    );

    const summary = db.$with('summary').as(
      db.select({
        category: (base as any).category,
        totalPrice: sum((base as any).price).as('total_price'),
        cnt: count().as('cnt'),
      })
        .from(base)
        .groupBy((base as any).category),
    );

    const result = await db.with(base, summary)
      .select()
      .from(summary)
      .orderBy(sql`${(summary as any).totalPrice} DESC`) as any;

    expect(result).toHaveLength(3);
  });

  it('should handle CTE with UNION ALL via raw SQL', async () => {
    const combined = db.$with('combined').as(
      sql`SELECT "name", 'product' AS source FROM "drizzle_test_products"
          UNION ALL
          SELECT "name", 'event' AS source FROM "drizzle_test_events"`,
    );

    const result = await db.with(combined)
      .select({
        source: sql<string>`source`,
        cnt: sql<number>`COUNT(*)`.as('cnt'),
      })
      .from(combined)
      .groupBy(sql`source`)
      .orderBy(sql`source`) as any;

    expect(result).toHaveLength(2);
  });
});
