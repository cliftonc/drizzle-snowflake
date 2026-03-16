# drizzle-snowflake

> **Status: Early Beta** -- API may change. Not yet tested against a live Snowflake instance. Unit tests cover the dialect, parameter handling, and column type serialization, but end-to-end integration tests still require Snowflake credentials.

A [Drizzle ORM](https://orm.drizzle.team/) driver for [Snowflake](https://www.snowflake.com/). Built on Drizzle's Postgres driver surface (`pg-core`) since Snowflake uses double-quote identifier quoting and has a broadly Postgres-compatible SQL dialect.

Uses [`snowflake-sdk`](https://www.npmjs.com/package/snowflake-sdk) as the underlying client. The SDK's callback-based API is wrapped in Promises internally.

## Install

```sh
bun add drizzle-snowflake drizzle-orm snowflake-sdk
```

## Quick start

```ts
import { drizzle } from 'drizzle-snowflake';
import { pgTable, integer, varchar, text } from 'drizzle-orm/pg-core';
import { eq } from 'drizzle-orm';

const users = pgTable('users', {
  id: integer('id').notNull(),
  name: varchar('name', { length: 256 }).notNull(),
  email: text('email'),
});

const db = await drizzle({
  connection: {
    account: 'my_account',
    username: 'my_user',
    password: 'my_password',
    database: 'MY_DB',
    warehouse: 'COMPUTE_WH',
    schema: 'PUBLIC',
  },
});

// Insert
await db.insert(users).values({ id: 1, name: 'Alice', email: 'alice@example.com' });

// Select
const rows = await db.select().from(users).where(eq(users.name, 'Alice'));

// Clean up
await db.close();
```

## Snowflake-specific column types

```ts
import {
  snowflakeVariant,
  snowflakeArray,
  snowflakeObject,
  snowflakeTimestampLtz,
  snowflakeTimestampNtz,
  snowflakeTimestampTz,
  snowflakeDate,
  snowflakeGeography,
  snowflakeNumber,
} from 'drizzle-snowflake';

const events = pgTable('events', {
  id: integer('id').notNull(),
  payload: snowflakeVariant('payload'),             // VARIANT (semi-structured)
  tags: snowflakeArray('tags'),                     // ARRAY
  attrs: snowflakeObject('attrs'),                  // OBJECT
  createdAt: snowflakeTimestampLtz('created_at'),   // TIMESTAMP_LTZ
  scheduledAt: snowflakeTimestampNtz('scheduled_at'), // TIMESTAMP_NTZ
  sentAt: snowflakeTimestampTz('sent_at'),          // TIMESTAMP_TZ
  eventDate: snowflakeDate('event_date'),           // DATE
  location: snowflakeGeography('location'),         // GEOGRAPHY
  amount: snowflakeNumber('amount', 18, 2),         // NUMBER(18, 2)
});
```

## Connection options

```ts
// Connection config object (async, auto-pools with 4 connections)
const db = await drizzle({
  connection: {
    account: 'my_account',
    username: 'my_user',
    password: 'my_password',
    database: 'MY_DB',
    warehouse: 'COMPUTE_WH',
  },
});

// With pool/logger config
const db = await drizzle({
  connection: { account: '...', username: '...', password: '...' },
  pool: { size: 8 },
  logger: true,
});

// Explicit client (sync, no pooling)
import snowflake from 'snowflake-sdk';
import { promisifyConnect } from 'drizzle-snowflake';

const conn = snowflake.createConnection({ account: '...', username: '...', password: '...' });
await promisifyConnect(conn);
const db = drizzle({ client: conn });

// Disable pooling
const db = await drizzle({
  connection: { account: '...', username: '...', password: '...' },
  pool: false,
});
```

## Transactions

```ts
await db.transaction(async (tx) => {
  await tx.insert(users).values({ id: 2, name: 'Bob', email: 'bob@example.com' });
  await tx.update(users).set({ name: 'Robert' }).where(eq(users.id, 2));
});
```

Note: Snowflake does not support savepoints. Nested transactions use a rollback-only fallback.

## Migrations

```ts
import { migrate } from 'drizzle-snowflake';

await migrate(db, { migrationsFolder: './drizzle' });
```

## Development

### Testing without Snowflake credentials

Unit tests cover the dialect (`$1` to `?` parameter rewriting), parameter preparation, and all column type serialization/deserialization. These run without any network access:

```sh
bun install
bun test test/dialect.test.ts test/client.test.ts test/columns.test.ts
```

### Integration tests (requires Snowflake)

There is no local Snowflake emulator or Docker image. Integration tests require a live Snowflake account. The easiest option is a [free 30-day trial](https://signup.snowflake.com/).

Set the following environment variables:

```sh
export SNOWFLAKE_ACCOUNT=my_account      # e.g. xy12345.us-east-1
export SNOWFLAKE_USER=my_user
export SNOWFLAKE_PASSWORD=my_password
export SNOWFLAKE_DATABASE=TEST_DB        # optional, defaults to TEST_DB
export SNOWFLAKE_WAREHOUSE=COMPUTE_WH    # optional, defaults to COMPUTE_WH
export SNOWFLAKE_SCHEMA=PUBLIC           # optional, defaults to PUBLIC
```

Then run:

```sh
bun test test/snowflake.test.ts
```

### Commands

```sh
bun install           # Install dependencies
bun run build         # Build (dist/index.mjs + type declarations)
bun test              # Run all tests
```

## Key differences from Postgres

- **Parameter placeholders**: Drizzle generates `$1, $2, ...` (Postgres style). The dialect rewrites these to `?` (Snowflake's positional params) automatically.
- **No savepoints**: Nested transactions fall back to rollback-only semantics.
- **No sequences**: Migrations use `COALESCE(MAX(id), 0) + 1` for ID generation.
- **Semi-structured types**: Use `snowflakeVariant`, `snowflakeArray`, `snowflakeObject` instead of Postgres JSON/JSONB.
- **Three timestamp types**: `TIMESTAMP_LTZ` (local), `TIMESTAMP_NTZ` (no timezone), `TIMESTAMP_TZ` (with timezone).

## Architecture

Built on the same pattern as [drizzle-databend](https://github.com/cliftonc/drizzle-databend):

- **`driver.ts`** -- `drizzle()` factory and `SnowflakeDatabase` extending `PgDatabase`
- **`session.ts`** -- `SnowflakeSession` and `SnowflakePreparedQuery` for query execution
- **`dialect.ts`** -- `SnowflakeDialect` extending `PgDialect` with `$N` to `?` rewriting, Snowflake-specific migrations
- **`client.ts`** -- Promisified wrappers around `snowflake-sdk`'s callback-based API
- **`pool.ts`** -- Connection pooling with acquire/release lifecycle
- **`columns.ts`** -- Custom column types (VARIANT, ARRAY, OBJECT, TIMESTAMP_LTZ/NTZ/TZ, DATE, GEOGRAPHY, NUMBER)
- **`sql/result-mapper.ts`** -- Maps Snowflake results to Drizzle's expected format

## License

MIT
