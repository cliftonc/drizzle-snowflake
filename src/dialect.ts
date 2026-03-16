import { entityKind, is } from 'drizzle-orm/entity';
import type { MigrationConfig, MigrationMeta } from 'drizzle-orm/migrator';
import {
  PgDate,
  PgDateString,
  PgDialect,
  PgNumeric,
  PgSession,
  PgTime,
  PgTimestamp,
  PgTimestampString,
  PgUUID,
} from 'drizzle-orm/pg-core';
import {
  sql,
  SQL,
  type DriverValueEncoder,
  type QueryTypingsValue,
  type QueryWithTypings,
} from 'drizzle-orm';

export class SnowflakeDialect extends PgDialect {
  static readonly [entityKind]: string = 'SnowflakePgDialect';

  // Snowflake does not support savepoints
  areSavepointsUnsupported(): boolean {
    return true;
  }

  override sqlToQuery(
    sqlObj: SQL,
    invokeSource?: 'indexes' | undefined
  ): QueryWithTypings {
    const result = super.sqlToQuery(sqlObj, invokeSource);
    // Rewrite $1, $2, ... to ? for Snowflake's positional parameter style
    result.sql = result.sql.replace(/\$(\d+)/g, '?');
    return result;
  }

  override async migrate(
    migrations: MigrationMeta[],
    session: PgSession,
    config: MigrationConfig | string
  ): Promise<void> {
    const migrationConfig: MigrationConfig =
      typeof config === 'string' ? { migrationsFolder: config } : config;

    const migrationsSchema = migrationConfig.migrationsSchema ?? 'PUBLIC';
    const migrationsTable =
      migrationConfig.migrationsTable ?? '__drizzle_migrations';

    // Snowflake has no sequences. Use explicit ID via COALESCE(MAX(id), 0) + 1.
    const migrationTableCreate = sql`
      CREATE TABLE IF NOT EXISTS ${sql.identifier(migrationsSchema)}.${sql.identifier(
        migrationsTable
      )} (
        id INT NOT NULL,
        hash VARCHAR NOT NULL,
        created_at BIGINT
      )
    `;

    await session.execute(migrationTableCreate);

    const dbMigrations = await session.all<{
      id: number;
      hash: string;
      created_at: string;
    }>(
      sql`SELECT id, hash, created_at FROM ${sql.identifier(
        migrationsSchema
      )}.${sql.identifier(migrationsTable)} ORDER BY created_at DESC LIMIT 1`
    );

    const lastDbMigration = dbMigrations[0];

    await session.transaction(async (tx) => {
      for await (const migration of migrations) {
        if (
          !lastDbMigration ||
          Number(lastDbMigration.created_at) < migration.folderMillis
        ) {
          for (const stmt of migration.sql) {
            await tx.execute(sql.raw(stmt));
          }

          await tx.execute(
            sql`INSERT INTO ${sql.identifier(
              migrationsSchema
            )}.${sql.identifier(migrationsTable)} (id, hash, created_at)
              VALUES (
                (SELECT COALESCE(MAX(id), 0) + 1 FROM ${sql.identifier(
                  migrationsSchema
                )}.${sql.identifier(migrationsTable)}),
                ${migration.hash},
                ${migration.folderMillis}
              )`
          );
        }
      }
    });
  }

  override prepareTyping(
    encoder: DriverValueEncoder<unknown, unknown>
  ): QueryTypingsValue {
    if (is(encoder, PgNumeric)) {
      return 'decimal';
    } else if (is(encoder, PgTime)) {
      return 'time';
    } else if (is(encoder, PgTimestamp) || is(encoder, PgTimestampString)) {
      return 'timestamp';
    } else if (is(encoder, PgDate) || is(encoder, PgDateString)) {
      return 'date';
    } else if (is(encoder, PgUUID)) {
      return 'uuid';
    } else {
      return 'none';
    }
  }
}
