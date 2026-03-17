import { entityKind } from 'drizzle-orm/entity';
import { SelectionProxyHandler } from 'drizzle-orm/selection-proxy';
import { sql } from 'drizzle-orm/sql/sql';
import { WithSubquery } from 'drizzle-orm/subquery';
import type { SnowflakeDialect } from './dialect.ts';
import { SnowflakeCountBuilder } from './query-builders/count.ts';
import {
  QueryBuilder,
  SnowflakeDeleteBase,
  SnowflakeInsertBuilder,
  SnowflakeSelectBuilder,
  SnowflakeUpdateBuilder,
} from './query-builders/index.ts';
import { SnowflakeRaw } from './query-builders/raw.ts';
import type { SnowflakeSession } from './session.ts';

export class SnowflakeDatabase<
  TFullSchema extends Record<string, unknown> = Record<string, never>,
  TSchema extends Record<string, any> = Record<string, never>,
> {
  static readonly [entityKind]: string = 'SnowflakeDatabase';

  declare readonly _: {
    readonly schema: TSchema | undefined;
    readonly fullSchema: Record<string, unknown>;
    readonly tableNamesMap: Record<string, string>;
    readonly session: SnowflakeSession<TFullSchema, TSchema>;
  };

  query: Record<string, any>;

  constructor(
    public dialect: SnowflakeDialect,
    public session: SnowflakeSession<TFullSchema, TSchema>,
    schema: any | undefined,
  ) {
    this._ = schema
      ? {
          schema: schema.schema,
          fullSchema: schema.fullSchema,
          tableNamesMap: schema.tableNamesMap,
          session,
        }
      : {
          schema: undefined,
          fullSchema: {},
          tableNamesMap: {},
          session,
        };
    this.query = {};
  }

  $with(alias: string) {
    const self = this;
    return {
      as(qb: any) {
        if (typeof qb === 'function') {
          qb = qb(new QueryBuilder(self.dialect));
        }
        return new Proxy(
          new WithSubquery(
            qb.getSQL(),
            qb.getSelectedFields ? qb.getSelectedFields() : {},
            alias,
            true,
          ),
          new SelectionProxyHandler({
            alias,
            sqlAliasedBehavior: 'alias',
            sqlBehavior: 'error',
          }),
        );
      },
    };
  }

  $count(source: any, filters?: any) {
    return new SnowflakeCountBuilder({ source, filters, session: this.session });
  }

  with(...queries: any[]) {
    const self = this;

    function select(fields?: any) {
      return new SnowflakeSelectBuilder({
        fields: fields ?? undefined,
        session: self.session,
        dialect: self.dialect,
        withList: queries,
      });
    }

    function selectDistinct(fields?: any) {
      return new SnowflakeSelectBuilder({
        fields: fields ?? undefined,
        session: self.session,
        dialect: self.dialect,
        withList: queries,
        distinct: true,
      });
    }

    function update(table: any) {
      return new SnowflakeUpdateBuilder(table, self.session, self.dialect, queries);
    }

    function insert(table: any) {
      return new SnowflakeInsertBuilder(table, self.session, self.dialect, queries);
    }

    function delete_(table: any) {
      return new SnowflakeDeleteBase(table, self.session, self.dialect, queries);
    }

    return { select, selectDistinct, update, insert, delete: delete_ };
  }

  select(fields?: any) {
    return new SnowflakeSelectBuilder({
      fields: fields ?? undefined,
      session: this.session,
      dialect: this.dialect,
    });
  }

  selectDistinct(fields?: any) {
    return new SnowflakeSelectBuilder({
      fields: fields ?? undefined,
      session: this.session,
      dialect: this.dialect,
      distinct: true,
    });
  }

  update(table: any) {
    return new SnowflakeUpdateBuilder(table, this.session, this.dialect);
  }

  insert(table: any) {
    return new SnowflakeInsertBuilder(table, this.session, this.dialect);
  }

  delete(table: any) {
    return new SnowflakeDeleteBase(table, this.session, this.dialect);
  }

  execute(query: any) {
    const sequel = typeof query === 'string' ? sql.raw(query) : query.getSQL ? query.getSQL() : query;
    const builtQuery = this.dialect.sqlToQuery(sequel);
    const prepared = this.session.prepareQuery(builtQuery, undefined, undefined, false);
    return new SnowflakeRaw(
      () => prepared.execute(undefined),
      sequel,
      builtQuery,
      (result: any) => prepared.mapResult(result, true),
    );
  }

  transaction(transaction: any): Promise<any> {
    return this.session.transaction(transaction);
  }
}
