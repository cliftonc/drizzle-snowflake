import { sql } from 'drizzle-orm';
import {
  snowflakeTimestampNtz,
  snowflakeVariant,
} from '../src/columns.ts';
import {
  boolean,
  doublePrecision,
  drizzle,
  integer,
  real,
  type SnowflakeDatabase,
  snowflakeTable,
  text,
  varchar,
} from '../src/index.ts';

// -- Table definitions --

export const users = snowflakeTable('drizzle_test_users', {
  id: integer('id').notNull(),
  name: varchar('name', { length: 256 }).notNull(),
  email: text('email'),
  active: boolean('active').default(true),
  score: real('score'),
});

export const products = snowflakeTable('drizzle_test_products', {
  id: integer('id').notNull(),
  name: varchar('name', { length: 256 }).notNull(),
  quantity: integer('quantity').notNull(),
  price: doublePrecision('price').notNull(),
  category: varchar('category', { length: 100 }),
  active: boolean('active').default(true),
  createdAt: snowflakeTimestampNtz('created_at'),
});

export const events = snowflakeTable('drizzle_test_events', {
  id: integer('id').notNull(),
  name: varchar('name', { length: 256 }).notNull(),
  startedAt: snowflakeTimestampNtz('started_at'),
  endedAt: snowflakeTimestampNtz('ended_at'),
  category: varchar('category', { length: 100 }),
  value: real('value'),
});

export const items = snowflakeTable('drizzle_test_items', {
  id: integer('id').notNull(),
  name: varchar('name', { length: 256 }).notNull(),
  metadata: snowflakeVariant('metadata'),
  createdAt: snowflakeTimestampNtz('created_at'),
});

// -- Shared db instance (created once, reused across all test files) --

export const hasCredentials = !!(
  process.env.SNOWFLAKE_ACCOUNT &&
  process.env.SNOWFLAKE_USER &&
  process.env.SNOWFLAKE_PASSWORD
);

let _db: SnowflakeDatabase | undefined;

export async function getDb(): Promise<SnowflakeDatabase> {
  if (!_db) {
    const account = process.env.SNOWFLAKE_ACCOUNT;
    const username = process.env.SNOWFLAKE_USER;
    const password = process.env.SNOWFLAKE_PASSWORD;
    const database = process.env.SNOWFLAKE_DATABASE ?? 'TESTDB';
    const warehouse = process.env.SNOWFLAKE_WAREHOUSE ?? 'COMPUTE_WH';
    const schema = process.env.SNOWFLAKE_SCHEMA ?? 'PUBLIC';

    if (!account || !username || !password) {
      throw new Error(
        'SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, and SNOWFLAKE_PASSWORD env vars are required',
      );
    }

    _db = await drizzle({
      connection: { account, username, password, database, warehouse, schema },
    });
  }
  return _db;
}

export async function closeDb(): Promise<void> {
  if (_db) {
    await _db.close();
    _db = undefined;
  }
}

// -- One-time setup: create tables and seed all data --

export async function setupAll(): Promise<void> {
  const database = process.env.SNOWFLAKE_DATABASE ?? 'TESTDB';

  // Ensure database exists — use a temporary non-pooled connection
  // (without a database set) to create it before the pool connects
  const snowflakeSdk = await import('snowflake-sdk');
  const { promisifyConnect, promisifyExecute, closeClientConnection } = await import('../src/client.ts');
  const tempConn = snowflakeSdk.default.createConnection({
    account: process.env.SNOWFLAKE_ACCOUNT!,
    username: process.env.SNOWFLAKE_USER!,
    password: process.env.SNOWFLAKE_PASSWORD!,
    warehouse: process.env.SNOWFLAKE_WAREHOUSE ?? 'COMPUTE_WH',
  });
  await promisifyConnect(tempConn);
  await promisifyExecute(tempConn, `CREATE DATABASE IF NOT EXISTS ${database}`, []);
  await closeClientConnection(tempConn);

  // Now create the pool — database exists so all connections will use it
  const db = await getDb();

  // Drop existing tables (quoted to match Drizzle's quoted escapeName)
  await db.execute(sql`DROP TABLE IF EXISTS "drizzle_test_users"`);
  await db.execute(sql`DROP TABLE IF EXISTS "drizzle_test_products"`);
  await db.execute(sql`DROP TABLE IF EXISTS "drizzle_test_events"`);
  await db.execute(sql`DROP TABLE IF EXISTS "drizzle_test_items"`);

  await db.execute(sql`
    CREATE TABLE "drizzle_test_users" (
      "id" INT NOT NULL,
      "name" VARCHAR(256) NOT NULL,
      "email" VARCHAR NULL,
      "active" BOOLEAN DEFAULT TRUE,
      "score" FLOAT NULL
    )
  `);
  await db.execute(sql`
    CREATE TABLE "drizzle_test_products" (
      "id" INT NOT NULL,
      "name" VARCHAR(256) NOT NULL,
      "quantity" INT NOT NULL,
      "price" DOUBLE NOT NULL,
      "category" VARCHAR(100) NULL,
      "active" BOOLEAN DEFAULT TRUE,
      "created_at" TIMESTAMP_NTZ NULL
    )
  `);
  await db.execute(sql`
    CREATE TABLE "drizzle_test_events" (
      "id" INT NOT NULL,
      "name" VARCHAR(256) NOT NULL,
      "started_at" TIMESTAMP_NTZ NULL,
      "ended_at" TIMESTAMP_NTZ NULL,
      "category" VARCHAR(100) NULL,
      "value" FLOAT NULL
    )
  `);
  await db.execute(sql`
    CREATE TABLE "drizzle_test_items" (
      "id" INT NOT NULL,
      "name" VARCHAR(256) NOT NULL,
      "metadata" VARIANT NULL,
      "created_at" TIMESTAMP_NTZ NULL
    )
  `);

  // Seed users (4 rows)
  await db.insert(users).values([
    { id: 1, name: 'Alice', email: 'alice@test.com', active: true, score: 9.5 },
    { id: 2, name: 'Bob', email: 'bob@test.com', active: true, score: 8.0 },
    { id: 3, name: 'Charlie', email: null, active: false, score: 7.5 },
    { id: 4, name: "O'Brien", email: 'obrien@test.com', active: true, score: 6.0 },
  ]);

  // Seed products (5 rows)
  await db.insert(products).values([
    { id: 1, name: 'Widget', quantity: 10, price: 19.99, category: 'hardware', active: true, createdAt: new Date('2024-01-15T10:00:00Z') },
    { id: 2, name: 'Gadget', quantity: 5, price: 49.99, category: 'hardware', active: true, createdAt: new Date('2024-02-20T14:30:00Z') },
    { id: 3, name: 'Doohickey', quantity: 0, price: 9.99, category: 'accessories', active: false, createdAt: new Date('2024-03-10T08:15:00Z') },
    { id: 4, name: 'Thingamajig', quantity: 25, price: 4.99, category: 'accessories', active: true, createdAt: new Date('2024-03-10T08:15:00Z') },
    { id: 5, name: 'Whatchamacallit', quantity: 3, price: 99.99, category: 'electronics', active: true, createdAt: new Date('2024-04-01T12:00:00Z') },
  ]);

  // Seed events (4 rows)
  await db.insert(events).values([
    { id: 1, name: 'Meeting', startedAt: new Date('2024-01-15T09:00:00Z'), endedAt: new Date('2024-01-15T10:30:00Z'), category: 'work', value: 1.0 },
    { id: 2, name: 'Lunch', startedAt: new Date('2024-01-15T12:00:00Z'), endedAt: new Date('2024-01-15T13:00:00Z'), category: 'personal', value: 2.0 },
    { id: 3, name: 'Workshop', startedAt: new Date('2024-02-01T14:00:00Z'), endedAt: new Date('2024-02-01T17:00:00Z'), category: 'work', value: 3.0 },
    { id: 4, name: 'Conference', startedAt: new Date('2024-03-10T08:00:00Z'), endedAt: new Date('2024-03-12T18:00:00Z'), category: 'work', value: 5.0 },
  ]);

  // Seed items (2 rows) — use INSERT ... SELECT with PARSE_JSON for VARIANT columns
  await db.execute(
    sql`INSERT INTO "drizzle_test_items" ("id", "name", "metadata", "created_at")
        SELECT 1, 'Test Item', PARSE_JSON(${JSON.stringify({ tags: ['important', 'urgent'], priority: 1 })}), '2024-01-01 00:00:00'::TIMESTAMP_NTZ`,
  );
  await db.execute(
    sql`INSERT INTO "drizzle_test_items" ("id", "name", "metadata", "created_at")
        SELECT 2, 'Quoted Item', PARSE_JSON(${JSON.stringify({ note: "it's a test", tags: ["won't break"] })}), '2024-06-15 00:00:00'::TIMESTAMP_NTZ`,
  );
}

export async function teardownAll(): Promise<void> {
  const db = await getDb();
  await db.execute(sql`DROP TABLE IF EXISTS "drizzle_test_users"`);
  await db.execute(sql`DROP TABLE IF EXISTS "drizzle_test_products"`);
  await db.execute(sql`DROP TABLE IF EXISTS "drizzle_test_events"`);
  await db.execute(sql`DROP TABLE IF EXISTS "drizzle_test_items"`);
  await closeDb();
}
