import { eq, sql } from 'drizzle-orm';
import { beforeAll, describe, expect, it } from 'vitest';
import type { SnowflakeDatabase } from '../src/index.ts';
import { events, getDb, items, users } from './setup.ts';

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
});
