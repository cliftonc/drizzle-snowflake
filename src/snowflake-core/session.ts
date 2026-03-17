import { entityKind } from 'drizzle-orm/entity';
import { TransactionRollbackError } from 'drizzle-orm/errors';
// @ts-expect-error - tracer exists at runtime
import { tracer } from 'drizzle-orm/tracing';
import { SnowflakeDatabase } from './db.ts';
import type { SnowflakeDialect } from './dialect.ts';

export class SnowflakePreparedQuery<T = unknown> {
  static readonly [entityKind]: string = 'SnowflakePreparedQuery';

  /** @internal */
  joinsNotNullableMap?: Record<string, boolean>;
  authToken?: any;
  query: { sql: string; params: unknown[] };

  constructor(query: { sql: string; params: unknown[] }) {
    this.query = query;
  }

  getQuery(): { sql: string; params: unknown[] } {
    return this.query;
  }

  mapResult(response: any, _isFromBatch?: boolean): any {
    return response;
  }
}

export class SnowflakeSession<
  TFullSchema extends Record<string, unknown> = Record<string, never>,
  TSchema extends Record<string, any> = Record<string, never>,
> {
  static readonly [entityKind]: string = 'SnowflakeSession';

  dialect: SnowflakeDialect;

  constructor(dialect: SnowflakeDialect) {
    this.dialect = dialect;
  }

  execute(query: any): any {
    return tracer.startActiveSpan('drizzle.operation', () => {
      const prepared = tracer.startActiveSpan('drizzle.prepareQuery', () => {
        return this.prepareQuery(this.dialect.sqlToQuery(query), undefined, undefined, false);
      });
      return prepared.execute(undefined);
    });
  }

  all<T = any>(query: any): Promise<T[]> {
    return this.prepareQuery(this.dialect.sqlToQuery(query), undefined, undefined, false).all();
  }

  async count(sql: any): Promise<number> {
    const res = await this.execute(sql);
    return Number(res[0]['count']);
  }

  prepareQuery(
    _query: any,
    _fields: any,
    _name: string | undefined,
    _isResponseInArrayMode: boolean,
    _customResultMapper?: any,
  ): any {
    throw new Error('prepareQuery must be implemented by subclass');
  }

  transaction(_transaction: any, _config?: any): Promise<any> {
    throw new Error('transaction must be implemented by subclass');
  }
}

export class SnowflakeTransaction<
  TFullSchema extends Record<string, unknown> = Record<string, never>,
  TSchema extends Record<string, any> = Record<string, never>,
> extends SnowflakeDatabase<TFullSchema, TSchema> {
  static readonly [entityKind]: string = 'SnowflakeTransaction';

  declare schema: any;

  constructor(
    dialect: SnowflakeDialect,
    session: SnowflakeSession<TFullSchema, TSchema>,
    schema: any,
  ) {
    super(dialect, session, schema);
    this.schema = schema;
  }

  rollback(): never {
    throw new TransactionRollbackError();
  }
}
