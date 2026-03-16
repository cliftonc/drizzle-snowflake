# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is `drizzle-snowflake`, a Snowflake dialect adapter for drizzle-orm. It builds on Drizzle's Postgres driver surface but targets Snowflake, providing query building, migrations, and type inference for Snowflake via `snowflake-sdk`.

## Commands

- **Install dependencies:** `bun install`
- **Run all tests:** `bun test`
- **Run a single test file:** `bun test test/<filename>.test.ts`
- **Build:** `bun run build` (emits `dist/index.mjs` and type declarations)
- **Build declarations only:** `bun run build:declarations`

## Architecture

### Core Module Structure (`src/`)

- `driver.ts` - Main entry point with `drizzle()` factory and `SnowflakeDatabase` class extending `PgDatabase`
- `session.ts` - `SnowflakeSession` and `SnowflakePreparedQuery` for query execution, transaction handling
- `dialect.ts` - `SnowflakeDialect` extending `PgDialect` with Snowflake-specific SQL generation (rewrites `$1` to `?`)
- `columns.ts` - Snowflake-specific column helpers (`snowflakeVariant`, `snowflakeArray`, `snowflakeObject`, `snowflakeTimestampLtz`, etc.)
- `pool.ts` - Connection pooling with `createSnowflakeConnectionPool()`
- `client.ts` - Low-level client utilities with promisified snowflake-sdk wrappers
- `migrator.ts` - `migrate()` function for applying SQL migrations
- `sql/result-mapper.ts` - Converts query results to Drizzle's expected format
- `sql/selection.ts` - Selection/projection handling

### Key Design Decisions

1. **Built on Postgres Driver**: Extends `PgDialect`, `PgSession`, `PgDatabase` since Snowflake's SQL is largely Postgres-compatible with double-quote identifier quoting
2. **Parameter Rewriting**: `$1, $2, ...` placeholders are rewritten to `?` in `sqlToQuery()` since Snowflake uses positional `?` params
3. **Promisified SDK**: `snowflake-sdk` uses callback-based APIs; all driver interactions are wrapped in Promises
4. **Connection Pooling**: Snowflake connections are pooled (default size 4) for concurrent queries
5. **No Pg JSON/JSONB**: Use `snowflakeVariant()` or `snowflakeObject()` instead of Postgres JSON types
6. **No Savepoints/Sequences**: Snowflake does not support savepoints; migrations use `COALESCE(MAX(id), 0) + 1`

### Testing

Tests require `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD` environment variables.

## Important Conventions

- ESM only with explicit `.ts` extensions in imports
- Source uses `moduleResolution: bundler`
- Never edit files in `dist/` - they are generated
- Never use emojis in comments or code
- Be concise and to the point
