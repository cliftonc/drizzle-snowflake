import { eq, sql } from 'drizzle-orm';
import { beforeAll, describe, expect, it } from 'vitest';
import type { SnowflakeDatabase } from '../src/index.ts';
import { events, getDb, items, products, users } from './setup.ts';

describe('drizzle-snowflake', () => {
  let db: SnowflakeDatabase;

  beforeAll(async () => {
    db = await getDb();
  });

  it('should connect and execute raw SQL', async () => {
    const result = await db.execute(sql`SELECT 1 AS val`);
    expect(result).toBeDefined();
  });

  it('should select all rows', async () => {
    const result = await db.select().from(users) as any;
    expect(result).toHaveLength(4);
  });

  it('should filter with where clause', async () => {
    const result = await db.select().from(users).where(eq(users.name, 'Alice')) as any;
    expect(result).toHaveLength(1);
    expect(result[0]!.name).toBe('Alice');
    expect(result[0]!.email).toBe('alice@test.com');
  });

  it('should handle VARIANT column type', async () => {
    const result = await db.select().from(items).where(eq(items.name, 'Test Item')) as any;
    expect(result).toHaveLength(1);
    expect(result[0]!.name).toBe('Test Item');
  });

  it('should perform a cross join', async () => {
    const result = await db
      .select({
        userName: users.name,
        eventName: events.name,
      })
      .from(users)
      .crossJoin(events)
      .where(eq(users.name, 'Alice'))
      .orderBy(events.name) as any;

    // Alice crossed with 4 events
    expect(result).toHaveLength(4);
    for (const row of result) {
      expect(row.userName).toBe('Alice');
      expect(row.eventName).toBeDefined();
    }
  });

  it('should perform a cross join lateral', async () => {
    // For each user, find products more expensive than the user's score
    const expensiveProducts = db
      .select({
        pname: sql<string>`${products.name}`.as('pname'),
        pprice: sql<number>`${products.price}`.as('pprice'),
      })
      .from(products)
      .where(sql`${products.price} > ${users.score}`)
      .as('expensive');

    const result = await db
      .select({
        userName: users.name,
        productName: (expensiveProducts as any).pname,
      })
      .from(users)
      .crossJoinLateral(expensiveProducts)
      .where(eq(users.name, 'Alice'))
      .orderBy((expensiveProducts as any).pprice) as any;

    expect(result.length).toBeGreaterThan(0);
    for (const row of result) {
      expect(row.userName).toBe('Alice');
      expect(row.productName).toBeDefined();
    }
  });

  it('should perform a left join lateral', async () => {
    // For each user, get their most expensive matching product (or null)
    const topProduct = db
      .select({
        pname: sql<string>`${products.name}`.as('pname'),
        pprice: sql<number>`${products.price}`.as('pprice'),
      })
      .from(products)
      .where(sql`${products.price} > ${users.score}`)
      .as('top_product');

    const result = await db
      .select({
        userName: users.name,
        topProductName: (topProduct as any).pname,
      })
      .from(users)
      .leftJoinLateral(topProduct, sql`true`)
      .orderBy(users.name) as any;

    // All 4 users appear, each with their matching products (LEFT join preserves all users)
    expect(result.length).toBeGreaterThanOrEqual(4);
    // Every row should have a userName
    for (const row of result) {
      expect(row.userName).toBeDefined();
    }
    // All 4 users should be represented
    const userNames = new Set(result.map((r: any) => r.userName));
    expect(userNames.size).toBe(4);
  });
});
