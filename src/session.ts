import { entityKind } from 'drizzle-orm/entity';
import { TransactionRollbackError } from 'drizzle-orm/errors';
import type { Logger } from 'drizzle-orm/logger';
import { NoopLogger } from 'drizzle-orm/logger';
import type {
  RelationalSchemaConfig,
  TablesRelationalConfig,
} from 'drizzle-orm/relations';
import { fillPlaceholders, type Query, type SQL, sql } from 'drizzle-orm/sql/sql';
import type { Assume } from 'drizzle-orm/utils';
import type {
  RowData,
  SnowflakeClientLike,
  SnowflakeConnection,
  SnowflakeConnectionPool,
} from './client.ts';
import {
  executeArraysOnClient,
  executeOnClient,
  isPool,
  prepareParams,
} from './client.ts';
import type { SnowflakeDialect } from './snowflake-core/dialect.ts';
import {
  SnowflakePreparedQuery as SnowflakePreparedQueryBase,
  SnowflakeSession as SnowflakeSessionBase,
  SnowflakeTransaction as SnowflakeTransactionBase,
} from './snowflake-core/session.ts';
import { mapResultRow } from './sql/result-mapper.ts';

export type { RowData, SnowflakeClientLike } from './client.ts';

export interface SnowflakeTransactionConfig {
  isolationLevel?: 'read uncommitted' | 'read committed' | 'repeatable read' | 'serializable';
  accessMode?: 'read only' | 'read write';
}

export interface PreparedQueryConfig {
  execute: unknown;
  all: unknown;
  values: unknown;
  get: unknown;
}

export class SnowflakePreparedQuery<
  T extends PreparedQueryConfig = PreparedQueryConfig,
> extends SnowflakePreparedQueryBase {
  static override readonly [entityKind]: string = 'SnowflakePreparedQuery';

  constructor(
    private client: SnowflakeClientLike,
    private queryString: string,
    private params: unknown[],
    private logger: Logger,
    private fields: any[] | undefined,
    private _isResponseInArrayMode: boolean,
    private customResultMapper:
      | ((rows: unknown[][]) => T['execute'])
      | undefined,
  ) {
    super({ sql: queryString, params });
  }

  async execute(
    placeholderValues: Record<string, unknown> | undefined = {},
  ): Promise<T['execute']> {
    const params = prepareParams(
      fillPlaceholders(this.params, placeholderValues),
    );
    this.logger.logQuery(this.queryString, params);

    const { fields, joinsNotNullableMap, customResultMapper } =
      this as typeof this & { joinsNotNullableMap?: Record<string, boolean> };

    if (fields) {
      const { rows } = await executeArraysOnClient(
        this.client,
        this.queryString,
        params,
      );

      if (rows.length === 0) {
        return [] as T['execute'];
      }

      return customResultMapper
        ? customResultMapper(rows)
        : rows.map((row) =>
            mapResultRow<T['execute']>(fields, row, joinsNotNullableMap),
          );
    }

    const rows = await executeOnClient(this.client, this.queryString, params);

    return rows as T['execute'];
  }

  all(
    placeholderValues: Record<string, unknown> | undefined = {},
  ): Promise<T['all']> {
    return this.execute(placeholderValues);
  }

  isResponseInArrayMode(): boolean {
    return this._isResponseInArrayMode;
  }
}

export interface SnowflakeSessionOptions {
  logger?: Logger;
}

export class SnowflakeSession<
  TFullSchema extends Record<string, unknown> = Record<string, never>,
  TSchema extends TablesRelationalConfig = Record<string, never>,
> extends SnowflakeSessionBase<TFullSchema, TSchema> {
  static override readonly [entityKind]: string = 'SnowflakeSession';

  declare dialect: SnowflakeDialect;
  private logger: Logger;
  private rollbackOnly = false;

  constructor(
    private client: SnowflakeClientLike,
    dialect: SnowflakeDialect,
    private schema: RelationalSchemaConfig<TSchema> | undefined,
    private options: SnowflakeSessionOptions = {},
  ) {
    super(dialect);
    this.dialect = dialect;
    this.logger = options.logger ?? new NoopLogger();
  }

  override prepareQuery<T extends PreparedQueryConfig = PreparedQueryConfig>(
    query: Query,
    fields: any[] | undefined,
    name: string | undefined,
    isResponseInArrayMode: boolean,
    customResultMapper?: (rows: unknown[][]) => T['execute'],
  ): SnowflakePreparedQuery<T> {
    void name;
    return new SnowflakePreparedQuery(
      this.client,
      query.sql,
      query.params,
      this.logger,
      fields,
      isResponseInArrayMode,
      customResultMapper,
    );
  }

  override async transaction<T>(
    transaction: (tx: SnowflakeTransaction<TFullSchema, TSchema>) => Promise<T>,
    config?: SnowflakeTransactionConfig,
  ): Promise<T> {
    let pinnedConnection: SnowflakeConnection | undefined;
    let pool: SnowflakeConnectionPool | undefined;

    let clientForTx: SnowflakeClientLike = this.client;
    if (isPool(this.client)) {
      pool = this.client;
      pinnedConnection = await pool.acquire();
      clientForTx = pinnedConnection;
    }

    const session = new SnowflakeSession(
      clientForTx,
      this.dialect,
      this.schema,
      this.options,
    );

    const tx = new SnowflakeTransaction<TFullSchema, TSchema>(
      this.dialect,
      session,
      this.schema,
    );

    try {
      await tx.execute(sql`BEGIN`);

      if (config) {
        await tx.setTransaction(config);
      }

      try {
        const result = await transaction(tx);
        if (session.isRollbackOnly()) {
          await tx.execute(sql`ROLLBACK`);
          throw new TransactionRollbackError();
        }
        await tx.execute(sql`COMMIT`);
        return result;
      } catch (error) {
        await tx.execute(sql`ROLLBACK`);
        throw error;
      }
    } finally {
      if (pinnedConnection && pool) {
        await pool.release(pinnedConnection);
      }
    }
  }

  markRollbackOnly(): void {
    this.rollbackOnly = true;
  }

  isRollbackOnly(): boolean {
    return this.rollbackOnly;
  }
}

const VALID_TRANSACTION_ISOLATION_LEVELS = new Set<string>([
  'read uncommitted',
  'read committed',
  'repeatable read',
  'serializable',
]);

const VALID_TRANSACTION_ACCESS_MODES = new Set<string>([
  'read only',
  'read write',
]);

export class SnowflakeTransaction<
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
> extends SnowflakeTransactionBase<TFullSchema, TSchema> {
  static override readonly [entityKind]: string = 'SnowflakeTransaction';

  declare nestedIndex: number;

  override rollback(): never {
    throw new TransactionRollbackError();
  }

  getTransactionConfigSQL(config: SnowflakeTransactionConfig): SQL {
    if (
      config.isolationLevel &&
      !VALID_TRANSACTION_ISOLATION_LEVELS.has(config.isolationLevel)
    ) {
      throw new Error(
        `Invalid transaction isolation level "${config.isolationLevel}". Expected one of: ${Array.from(
          VALID_TRANSACTION_ISOLATION_LEVELS,
        ).join(', ')}.`,
      );
    }

    if (
      config.accessMode &&
      !VALID_TRANSACTION_ACCESS_MODES.has(config.accessMode)
    ) {
      throw new Error(
        `Invalid transaction access mode "${config.accessMode}". Expected one of: ${Array.from(
          VALID_TRANSACTION_ACCESS_MODES,
        ).join(', ')}.`,
      );
    }

    const chunks: string[] = [];
    if (config.isolationLevel) {
      chunks.push(`isolation level ${config.isolationLevel}`);
    }
    if (config.accessMode) {
      chunks.push(config.accessMode);
    }
    return sql.raw(chunks.join(' '));
  }

  setTransaction(config: SnowflakeTransactionConfig): Promise<void> {
    return (this as any).session.execute(
      sql`SET TRANSACTION ${this.getTransactionConfigSQL(config)}`,
    );
  }

  override async transaction<T>(
    transaction: (tx: SnowflakeTransaction<TFullSchema, TSchema>) => Promise<T>,
  ): Promise<T> {
    // Snowflake does not support savepoints. Use rollback-only fallback.
    const internals = this as any;

    const nestedTx = new SnowflakeTransaction<TFullSchema, TSchema>(
      internals.dialect,
      internals.session,
      this.schema,
    );

    return transaction(nestedTx).catch((error) => {
      (internals.session as SnowflakeSession<TFullSchema, TSchema>).markRollbackOnly();
      throw error;
    });
  }
}

export type GenericRowData<T extends RowData = RowData> = T;

export type GenericTableData<T = RowData> = T[];

export interface SnowflakeQueryResultHKT {
  readonly $brand: 'SnowflakeQueryResultHKT';
  readonly row: unknown;
  type: GenericTableData<Assume<this['row'], RowData>>;
}
