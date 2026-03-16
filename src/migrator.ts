import type { MigrationConfig } from 'drizzle-orm/migrator';
import { readMigrationFiles } from 'drizzle-orm/migrator';
import type { SnowflakeDatabase } from './driver.ts';
import type { PgSession } from 'drizzle-orm/pg-core/session';

export type SnowflakeMigrationConfig = MigrationConfig | string;

export async function migrate<TSchema extends Record<string, unknown>>(
  db: SnowflakeDatabase<TSchema>,
  config: SnowflakeMigrationConfig
) {
  const migrationConfig: MigrationConfig =
    typeof config === 'string' ? { migrationsFolder: config } : config;

  const migrations = readMigrationFiles(migrationConfig);

  await db.dialect.migrate(
    migrations,
    db.session as unknown as PgSession,
    migrationConfig
  );
}
