import { and, between, count, eq, gt, sql } from 'drizzle-orm';
import { beforeAll, describe, expect, it } from 'vitest';
import type { SnowflakeDatabase } from '../src/index.ts';
import { events, getDb, products } from './setup.ts';

describe('date functions', () => {
  let db: SnowflakeDatabase;

  beforeAll(async () => {
    db = await getDb();
  });

  describe('DATE_TRUNC', () => {
    it('should truncate to month in a grouped select', async () => {
      const result = await db
        .select({
          monthStart: sql<string>`DATE_TRUNC('MONTH', ${products.createdAt})`.as('month_start'),
          cnt: count(),
        })
        .from(products)
        .groupBy(sql`DATE_TRUNC('MONTH', ${products.createdAt})`)
        .orderBy(sql`DATE_TRUNC('MONTH', ${products.createdAt})`) as any;
      expect(result.length).toBeGreaterThanOrEqual(3);
    });

    it('should truncate to year in a grouped select', async () => {
      const result = await db
        .select({
          yearStart: sql<string>`DATE_TRUNC('YEAR', ${products.createdAt})`.as('year_start'),
          cnt: count(),
        })
        .from(products)
        .groupBy(sql`DATE_TRUNC('YEAR', ${products.createdAt})`) as any;
      expect(result.length).toBeGreaterThanOrEqual(1);
    });

    it('should filter using DATE_TRUNC in where clause', async () => {
      const result = await db
        .select()
        .from(products)
        .where(eq(sql`DATE_TRUNC('MONTH', ${products.createdAt})`, sql`'2024-03-01'::TIMESTAMP_NTZ`)) as any;
      expect(result).toHaveLength(2);
    });
  });

  describe('DATEDIFF', () => {
    it('should calculate difference in seconds', async () => {
      // Use raw column names to avoid result mapper treating output as timestamp
      const result = await db
        .select({
          name: events.name,
          durationSecs: sql<number>`DATEDIFF('second', "started_at", "ended_at")`.as('duration_secs'),
        })
        .from(events)
        .orderBy(events.id) as any;
      expect(result).toHaveLength(4);
      // Meeting: 1.5 hours = 5400 seconds
      expect(Number(result[0]!.durationSecs)).toBe(5400);
      // Lunch: 1 hour = 3600 seconds
      expect(Number(result[1]!.durationSecs)).toBe(3600);
    });

    it('should calculate difference in hours', async () => {
      const result = await db
        .select({
          name: events.name,
          durationHours: sql<number>`DATEDIFF('hour', "started_at", "ended_at")`.as('duration_hours'),
        })
        .from(events)
        .where(eq(events.name, 'Workshop')) as any;
      expect(result).toHaveLength(1);
      expect(Number(result[0]!.durationHours)).toBe(3);
    });

    it('should filter by computed duration', async () => {
      // Events longer than 5000 seconds (> ~83 minutes)
      const result = await db
        .select({
          name: events.name,
          durationSecs: sql<number>`DATEDIFF('second', "started_at", "ended_at")`.as('duration_secs'),
        })
        .from(events)
        .where(gt(sql`DATEDIFF('second', "started_at", "ended_at")`, 5000)) as any;
      expect(result.length).toBeGreaterThanOrEqual(2);
    });
  });

  describe('date comparisons with params', () => {
    it('should filter with gt() on timestamp column', async () => {
      const cutoff = new Date('2024-02-01T00:00:00Z');
      const result = await db
        .select()
        .from(products)
        .where(gt(products.createdAt, cutoff)) as any;
      expect(result).toHaveLength(4);
    });

    it('should filter with between() on timestamp column', async () => {
      const start = new Date('2024-01-01T00:00:00Z');
      const end = new Date('2024-02-28T23:59:59Z');
      const result = await db
        .select()
        .from(products)
        .where(between(products.createdAt, start, end)) as any;
      expect(result).toHaveLength(2);
    });

    it('should combine date filter with other conditions', async () => {
      const cutoff = new Date('2024-03-01T00:00:00Z');
      const result = await db
        .select()
        .from(products)
        .where(and(gt(products.createdAt, cutoff), eq(products.active, true))) as any;
      expect(result).toHaveLength(2);
    });
  });
});
