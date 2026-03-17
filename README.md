# drizzle-snowflake

> This driver was created to power [drizzle-cube](https://try.drizzle-cube.dev) (an embeddable semantic layer built on Drizzle) and [drizby](https://github.com/cliftonc/drizby) (an open source BI platform built on drizzle-cube). It enables both projects to query Snowflake natively via Drizzle ORM.

A [Drizzle ORM](https://orm.drizzle.team/) driver for [Snowflake](https://www.snowflake.com/). Built on a standalone `snowflake-core` dialect module with `?` positional parameters and double-quote identifier quoting.

Uses [`snowflake-sdk`](https://github.com/snowflakedb/snowflake-connector-nodejs) as the underlying client.

## Install

```sh
npm install drizzle-snowflake drizzle-orm snowflake-sdk
```

## Quick start

```ts
import { drizzle, snowflakeTable, integer, varchar, text } from 'drizzle-snowflake';
import { eq } from 'drizzle-orm';

const users = snowflakeTable('users', {
  id: integer('id').notNull(),
  name: varchar('name', { length: 256 }).notNull(),
  email: text('email'),
});

const db = await drizzle({
  connection: {
    account: 'orgname-accountname',
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

## Column types

### Standard types

```ts
import {
  snowflakeTable,
  integer, bigint, smallint,
  real, doublePrecision, decimal,
  varchar, text, boolean,
  timestamp, date, json,
} from 'drizzle-snowflake';

const products = snowflakeTable('products', {
  id: integer('id').notNull(),
  name: varchar('name', { length: 256 }).notNull(),
  price: doublePrecision('price'),
  active: boolean('active').default(true),
  createdAt: timestamp('created_at'),
});
```

### Snowflake-specific types

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

const events = snowflakeTable('events', {
  id: integer('id').notNull(),
  payload: snowflakeVariant('payload'),           // VARIANT (semi-structured JSON)
  tags: snowflakeArray('tags'),                   // ARRAY
  attrs: snowflakeObject('attrs'),                // OBJECT
  createdAt: snowflakeTimestampNtz('created_at'), // TIMESTAMP_NTZ
  eventDate: snowflakeDate('event_date'),         // DATE
  amount: snowflakeNumber('amount', 18, 4),       // NUMBER(18, 4)
});
```

Note: inserting into VARIANT columns requires `PARSE_JSON()` -- use `INSERT ... SELECT` for VARIANT inserts:

```ts
import { sql } from 'drizzle-orm';

await db.execute(
  sql`INSERT INTO "events" ("id", "payload")
      SELECT 1, PARSE_JSON(${JSON.stringify({ key: 'value' })})`,
);
```

## Connection options

```ts
// Config object with connection (async, auto-pools with 4 connections)
const db = await drizzle({
  connection: {
    account: 'orgname-accountname',
    username: 'my_user',
    password: 'my_password',
    database: 'MY_DB',
    warehouse: 'COMPUTE_WH',
  },
});

// With pool and logger config
const db = await drizzle({
  connection: { account: '...', username: '...', password: '...' },
  pool: { size: 8 },
  logger: true,
});

// Disable pooling (single connection)
const db = await drizzle({
  connection: { account: '...', username: '...', password: '...' },
  pool: false,
});

// Explicit client (sync, no pooling)
import snowflake from 'snowflake-sdk';
import { promisifyConnect } from 'drizzle-snowflake';

const conn = snowflake.createConnection({ account: '...', username: '...', password: '...' });
await promisifyConnect(conn);
const db = drizzle({ client: conn });
```

## Transactions

```ts
await db.transaction(async (tx) => {
  await tx.insert(users).values({ id: 2, name: 'Bob', email: 'bob@example.com' });
  await tx.update(users).set({ name: 'Robert' }).where(eq(users.id, 2));
});
```

Snowflake does not support savepoints. Nested transactions use a rollback-only fallback.

## Migrations

```ts
import { migrate } from 'drizzle-snowflake';

await migrate(db, { migrationsFolder: './drizzle' });
```

## Development

### Prerequisites

You need a Snowflake account. Sign up for a [free trial](https://signup.snowflake.com/) if you don't have one.

### Environment setup

Create a `.env` file in the project root with your Snowflake credentials:

```
SNOWFLAKE_ACCOUNT=orgname-accountname
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=TESTDB
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_SCHEMA=PUBLIC
```

The `.env` file is gitignored and loaded automatically by the test setup via [dotenv](https://www.npmjs.com/package/dotenv).

Your account identifier is in the format `orgname-accountname` (visible in the Snowflake UI URL).

### Commands

```sh
npm install             # Install dependencies
npm run build           # Build (dist/index.mjs + type declarations)
npm run typecheck       # Type-check with tsc
npm run lint            # Lint with biome
npm test                # Run all tests (unit + integration)
```

Unit tests (`dialect.test.ts`, `columns.test.ts`, `client.test.ts`) run without Snowflake credentials. Integration tests require the `.env` file.

### Test suite

119 tests across 11 files:

| File | What it covers |
|---|---|
| `dialect.test.ts` | Parameter style (`?`), identifier quoting |
| `columns.test.ts` | Column type toDriver/fromDriver mappings |
| `client.test.ts` | Parameter preparation, pool detection |
| `snowflake.test.ts` | Connection, CRUD, VARIANT, cross joins |
| `aggregates.test.ts` | count, avg, sum, min, max, COALESCE, CASE WHEN, STDDEV, VAR |
| `type-coercion.test.ts` | Integer/double comparisons with gt, lt, gte, eq |
| `like-lower.test.ts` | LIKE patterns, LOWER(), case-insensitive search |
| `date-functions.test.ts` | DATE_TRUNC, DATEDIFF, date comparisons, BETWEEN |
| `params.test.ts` | String/int/bool/float binding, SQL injection prevention |
| `concurrent.test.ts` | Promise.all, pool stress with size=2 |
| `cte.test.ts` | CTEs with aggregation, LEFT JOIN, multiple CTEs, UNION ALL |

## Key differences from Postgres

- **No pg-core dependency**: Uses a standalone `snowflake-core` dialect -- no unsupported Postgres features leak into the API.
- **`?` parameters**: Snowflake uses positional `?` params natively (not `$1, $2, ...`).
- **No savepoints**: Nested transactions fall back to rollback-only semantics.
- **No sequences**: Migrations use `COALESCE(MAX(id), 0) + 1` for ID generation.
- **No RETURNING**: INSERT/UPDATE/DELETE do not support `RETURNING` clauses.
- **Semi-structured types**: Use `snowflakeVariant`, `snowflakeArray`, `snowflakeObject` instead of Postgres JSON/JSONB.
- **Three timestamp types**: `TIMESTAMP_LTZ` (local), `TIMESTAMP_NTZ` (no timezone), `TIMESTAMP_TZ` (with timezone).

## Architecture

- **`src/snowflake-core/`** -- Standalone dialect module (ported from drizzle-orm's gel-core)
  - `dialect.ts` -- SQL generation with `?` params and `"` identifier quoting
  - `session.ts` -- Abstract session, prepared query, and transaction base classes
  - `db.ts` -- `SnowflakeDatabase` with select/insert/update/delete/execute/CTE support
  - `table.ts` -- `snowflakeTable()` table definition function
  - `columns/` -- Column type builders (integer, varchar, boolean, timestamp, etc.)
  - `query-builders/` -- SELECT, INSERT, UPDATE, DELETE query builders
- **`src/driver.ts`** -- `drizzle()` factory, connection management, pool creation
- **`src/session.ts`** -- Concrete session with snowflake-sdk query execution
- **`src/client.ts`** -- Promisified snowflake-sdk wrappers, `rowMode: 'array'` for correct column mapping
- **`src/pool.ts`** -- Connection pooling with configurable size, timeouts, and recycling
- **`src/columns.ts`** -- Snowflake-specific column types (VARIANT, ARRAY, OBJECT, TIMESTAMP_LTZ/NTZ/TZ, etc.)
- **`src/migrator.ts`** -- Migration runner

## License

MIT
