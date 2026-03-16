import snowflake from 'snowflake-sdk';
import { entityKind } from 'drizzle-orm/entity';
import type { Logger } from 'drizzle-orm/logger';
import { DefaultLogger } from 'drizzle-orm/logger';
import { PgDatabase } from 'drizzle-orm/pg-core/db';
import {
  createTableRelationsHelpers,
  extractTablesRelationalConfig,
  type ExtractTablesWithRelations,
  type RelationalSchemaConfig,
  type TablesRelationalConfig,
} from 'drizzle-orm/relations';
import { type DrizzleConfig } from 'drizzle-orm/utils';
import type {
  SnowflakeClientLike,
  SnowflakeQueryResultHKT,
  SnowflakeTransaction,
} from './session.ts';
import { SnowflakeSession } from './session.ts';
import { SnowflakeDialect } from './dialect.ts';
import {
  isPool,
  closeClientConnection,
  promisifyConnect,
  type SnowflakeConnection,
} from './client.ts';
import {
  createSnowflakeConnectionPool,
  type SnowflakePoolConfig,
  type SnowflakeConnectionConfig,
} from './pool.ts';

export interface SnowflakeDriverOptions {
  logger?: Logger;
}

export class SnowflakeDriver {
  static readonly [entityKind]: string = 'SnowflakeDriver';

  constructor(
    private client: SnowflakeClientLike,
    private dialect: SnowflakeDialect,
    private options: SnowflakeDriverOptions = {}
  ) {}

  createSession(
    schema: RelationalSchemaConfig<TablesRelationalConfig> | undefined
  ): SnowflakeSession<Record<string, unknown>, TablesRelationalConfig> {
    return new SnowflakeSession(this.client, this.dialect, schema, {
      logger: this.options.logger,
    });
  }
}

export interface SnowflakeDrizzleConfig<
  TSchema extends Record<string, unknown> = Record<string, never>,
> extends DrizzleConfig<TSchema> {
  /** Pool configuration. Use size config or false to disable. */
  pool?: SnowflakePoolConfig | false;
}

export interface SnowflakeDrizzleConfigWithConnection<
  TSchema extends Record<string, unknown> = Record<string, never>,
> extends SnowflakeDrizzleConfig<TSchema> {
  /** Snowflake connection options */
  connection: SnowflakeConnectionConfig;
}

export interface SnowflakeDrizzleConfigWithClient<
  TSchema extends Record<string, unknown> = Record<string, never>,
> extends SnowflakeDrizzleConfig<TSchema> {
  /** Explicit client (connection or pool) */
  client: SnowflakeClientLike;
}

function isConfigObject(data: unknown): data is Record<string, unknown> {
  if (typeof data !== 'object' || data === null) return false;
  if (data.constructor?.name !== 'Object') return false;
  return (
    'connection' in data ||
    'client' in data ||
    'pool' in data ||
    'schema' in data ||
    'logger' in data
  );
}

function createFromClient<
  TSchema extends Record<string, unknown> = Record<string, never>,
>(
  client: SnowflakeClientLike,
  config: SnowflakeDrizzleConfig<TSchema> = {}
): SnowflakeDatabase<TSchema, ExtractTablesWithRelations<TSchema>> {
  const dialect = new SnowflakeDialect();

  const logger =
    config.logger === true ? new DefaultLogger() : config.logger || undefined;

  let schema: RelationalSchemaConfig<TablesRelationalConfig> | undefined;

  if (config.schema) {
    const tablesConfig = extractTablesRelationalConfig(
      config.schema,
      createTableRelationsHelpers
    );
    schema = {
      fullSchema: config.schema,
      schema: tablesConfig.tables,
      tableNamesMap: tablesConfig.tableNamesMap,
    };
  }

  const driver = new SnowflakeDriver(client, dialect, { logger });
  const session = driver.createSession(schema);

  const db = new SnowflakeDatabase(dialect, session, schema, client);
  return db as SnowflakeDatabase<TSchema, ExtractTablesWithRelations<TSchema>>;
}

async function createFromConnectionConfig<
  TSchema extends Record<string, unknown> = Record<string, never>,
>(
  connectionOptions: SnowflakeConnectionConfig,
  config: SnowflakeDrizzleConfig<TSchema> = {}
): Promise<SnowflakeDatabase<TSchema, ExtractTablesWithRelations<TSchema>>> {
  if (config.pool === false) {
    const conn = snowflake.createConnection(connectionOptions);
    const connection = await promisifyConnect(conn);
    return createFromClient(connection, config);
  }

  const poolSize = config.pool?.size ?? 4;
  const pool = createSnowflakeConnectionPool(connectionOptions, {
    size: poolSize,
  });
  return createFromClient(pool, config);
}

// Overload 1: Connection config (async, auto-pools)
export function drizzle<
  TSchema extends Record<string, unknown> = Record<string, never>,
>(
  connectionConfig: SnowflakeConnectionConfig
): Promise<SnowflakeDatabase<TSchema, ExtractTablesWithRelations<TSchema>>>;

// Overload 2: Connection config + drizzle config (async, auto-pools)
export function drizzle<
  TSchema extends Record<string, unknown> = Record<string, never>,
>(
  connectionConfig: SnowflakeConnectionConfig,
  config: SnowflakeDrizzleConfig<TSchema>
): Promise<SnowflakeDatabase<TSchema, ExtractTablesWithRelations<TSchema>>>;

// Overload 3: Config with connection (async, auto-pools)
export function drizzle<
  TSchema extends Record<string, unknown> = Record<string, never>,
>(
  config: SnowflakeDrizzleConfigWithConnection<TSchema>
): Promise<SnowflakeDatabase<TSchema, ExtractTablesWithRelations<TSchema>>>;

// Overload 4: Config with explicit client (sync)
export function drizzle<
  TSchema extends Record<string, unknown> = Record<string, never>,
>(
  config: SnowflakeDrizzleConfigWithClient<TSchema>
): SnowflakeDatabase<TSchema, ExtractTablesWithRelations<TSchema>>;

// Overload 5: Explicit client (sync, backward compatible)
export function drizzle<
  TSchema extends Record<string, unknown> = Record<string, never>,
>(
  client: SnowflakeClientLike,
  config?: SnowflakeDrizzleConfig<TSchema>
): SnowflakeDatabase<TSchema, ExtractTablesWithRelations<TSchema>>;

// Implementation
export function drizzle<
  TSchema extends Record<string, unknown> = Record<string, never>,
>(
  clientOrConfig:
    | SnowflakeConnectionConfig
    | SnowflakeClientLike
    | SnowflakeDrizzleConfigWithConnection<TSchema>
    | SnowflakeDrizzleConfigWithClient<TSchema>,
  config?: SnowflakeDrizzleConfig<TSchema>
):
  | SnowflakeDatabase<TSchema, ExtractTablesWithRelations<TSchema>>
  | Promise<SnowflakeDatabase<TSchema, ExtractTablesWithRelations<TSchema>>> {
  // Config object with connection or client
  if (isConfigObject(clientOrConfig)) {
    const configObj = clientOrConfig as
      | SnowflakeDrizzleConfigWithConnection<TSchema>
      | SnowflakeDrizzleConfigWithClient<TSchema>;

    if ('client' in configObj) {
      const clientConfig = configObj as SnowflakeDrizzleConfigWithClient<TSchema>;
      const { client: clientValue, ...restConfig } = clientConfig;
      return createFromClient(
        clientValue,
        restConfig as SnowflakeDrizzleConfig<TSchema>
      );
    }

    if ('connection' in configObj) {
      const connConfig =
        configObj as SnowflakeDrizzleConfigWithConnection<TSchema>;
      const { connection, ...restConfig } = connConfig;
      return createFromConnectionConfig(
        connection,
        restConfig as SnowflakeDrizzleConfig<TSchema>
      );
    }

    throw new Error(
      'Invalid drizzle config: either connection or client must be provided'
    );
  }

  // Check if it looks like a Snowflake connection config (has 'account' property)
  if (
    typeof clientOrConfig === 'object' &&
    clientOrConfig !== null &&
    'account' in clientOrConfig &&
    !('acquire' in clientOrConfig) &&
    !('execute' in clientOrConfig)
  ) {
    return createFromConnectionConfig(
      clientOrConfig as SnowflakeConnectionConfig,
      config
    );
  }

  // Direct client (backward compatible)
  return createFromClient(clientOrConfig as SnowflakeClientLike, config);
}

export class SnowflakeDatabase<
  TFullSchema extends Record<string, unknown> = Record<string, never>,
  TSchema extends TablesRelationalConfig =
    ExtractTablesWithRelations<TFullSchema>,
> extends PgDatabase<SnowflakeQueryResultHKT, TFullSchema, TSchema> {
  static readonly [entityKind]: string = 'SnowflakeDatabase';

  /** The underlying connection or pool */
  readonly $client: SnowflakeClientLike;

  constructor(
    readonly dialect: SnowflakeDialect,
    readonly session: SnowflakeSession<TFullSchema, TSchema>,
    schema: RelationalSchemaConfig<TSchema> | undefined,
    client: SnowflakeClientLike
  ) {
    super(dialect, session, schema);
    this.$client = client;
  }

  async close(): Promise<void> {
    if (isPool(this.$client) && this.$client.close) {
      await this.$client.close();
    }
    if (!isPool(this.$client)) {
      await closeClientConnection(this.$client as SnowflakeConnection);
    }
  }

  override async transaction<T>(
    transaction: (tx: SnowflakeTransaction<TFullSchema, TSchema>) => Promise<T>
  ): Promise<T> {
    return await this.session.transaction<T>(transaction);
  }
}
