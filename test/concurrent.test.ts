import { avg, count, eq, max, min, sum } from 'drizzle-orm';
import { beforeAll, describe, expect, it } from 'vitest';
import type { SnowflakeDatabase } from '../src/index.ts';
import { drizzle } from '../src/index.ts';
import { getDb, products, users } from './setup.ts';

describe('concurrent queries', () => {
  let db: SnowflakeDatabase;

  beforeAll(async () => {
    db = await getDb();
  });

  it('should handle multiple ORM selects via Promise.all', async () => {
    const [usersResult, productsResult, countResult] = await Promise.all([
      db.select().from(users),
      db.select().from(products),
      db.select({ cnt: count() }).from(users),
    ]) as any;

    expect(usersResult).toHaveLength(4);
    expect(productsResult).toHaveLength(5);
    expect(Number(countResult[0]!.cnt)).toBe(4);
  });

  it('should handle many concurrent filtered reads', async () => {
    const promises = Array.from({ length: 10 }, (_, i) =>
      db.select().from(users).where(eq(users.id, (i % 4) + 1)),
    );

    const results = await Promise.all(promises) as any;
    for (const result of results) {
      expect(result).toHaveLength(1);
    }
  });

  it('should handle concurrent aggregates', async () => {
    const results = await Promise.all([
      db.select({ cnt: count() }).from(users),
      db.select({ total: sum(products.price) }).from(products),
      db.select({ average: avg(products.quantity) }).from(products),
      db.select({ highest: max(users.score) }).from(users),
      db.select({ lowest: min(products.price) }).from(products),
    ]) as any;

    for (const result of results) {
      expect(result).toHaveLength(1);
    }
    expect(Number(results[0]![0]!.cnt)).toBe(4);
  });

  it('should handle concurrent mixed reads with filters', async () => {
    const results = await Promise.all([
      db.select().from(users).where(eq(users.active, true)),
      db.select({ cnt: count() }).from(products),
      db.select().from(products).where(eq(products.active, true)),
      db.select().from(users).where(eq(users.name, 'Alice')),
    ]) as any;

    expect(results[0]).toHaveLength(3);
    expect(Number(results[1]![0]!.cnt)).toBe(5);
    expect(results[2]).toHaveLength(4);
    expect(results[3]).toHaveLength(1);
  });
});

describe('pool stress under load', () => {
  let db: SnowflakeDatabase;

  beforeAll(async () => {
    const account = process.env.SNOWFLAKE_ACCOUNT!;
    const username = process.env.SNOWFLAKE_USER!;
    const password = process.env.SNOWFLAKE_PASSWORD!;
    const database = process.env.SNOWFLAKE_DATABASE ?? 'TESTDB';
    const warehouse = process.env.SNOWFLAKE_WAREHOUSE ?? 'COMPUTE_WH';
    const schema = process.env.SNOWFLAKE_SCHEMA ?? 'PUBLIC';

    // Create a pool with size=2 to maximize contention
    db = await drizzle({
      connection: { account, username, password, database, warehouse, schema },
      pool: { size: 2 },
    });
  });

  it('should handle 20 concurrent queries on pool size 2', async () => {
    const promises = Array.from({ length: 20 }, (_, i) =>
      db.select().from(users).where(eq(users.id, (i % 4) + 1)),
    );

    const results = await Promise.all(promises) as any;
    expect(results).toHaveLength(20);
    for (const result of results) {
      expect(result).toHaveLength(1);
    }
  });

  it('should handle concurrent queries with parameterized strings', async () => {
    const names = ['Alice', 'Bob', "O'Brien", 'Charlie'];
    const promises = Array.from({ length: 20 }, (_, i) =>
      db.select().from(users).where(eq(users.name, names[i % names.length]!)),
    );

    const results = await Promise.all(promises) as any;
    expect(results).toHaveLength(20);
    for (const result of results) {
      expect(result).toHaveLength(1);
    }
  });

  it('should handle concurrent mixed selects and aggregates', async () => {
    const promises = Array.from({ length: 15 }, (_, i) => {
      switch (i % 5) {
        case 0: return db.select().from(users);
        case 1: return db.select().from(products);
        case 2: return db.select({ cnt: count() }).from(users);
        case 3: return db.select({ avg: avg(products.price) }).from(products);
        default: return db.select().from(users).where(eq(users.active, true));
      }
    });

    const results = await Promise.all(promises) as any;
    expect(results).toHaveLength(15);
    for (const result of results) {
      expect(result.length).toBeGreaterThan(0);
    }
  });
});
