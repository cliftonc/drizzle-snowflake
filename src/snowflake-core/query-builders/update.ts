import { entityKind } from 'drizzle-orm/entity';
import { QueryPromise } from 'drizzle-orm/query-promise';
import { Table } from 'drizzle-orm/table';
// @ts-ignore
import { getTableLikeName, mapUpdateSet } from 'drizzle-orm/utils';

export class SnowflakeUpdateBuilder {
  static readonly [entityKind]: string = 'SnowflakeUpdateBuilder';

  authToken: any;

  constructor(
    public table: any,
    public session: any,
    public dialect: any,
    public withList?: any[],
  ) {}

  setToken(token: any): this {
    this.authToken = token;
    return this;
  }

  set(values: any) {
    return new SnowflakeUpdateBase(
      this.table,
      mapUpdateSet(this.table, values),
      this.session,
      this.dialect,
      this.withList,
    );
  }
}

export class SnowflakeUpdateBase extends QueryPromise<any> {
  static readonly [entityKind]: string = 'SnowflakeUpdate';

  config: any;
  tableName: string | undefined;
  joinsNotNullableMap: Record<string, boolean>;
  session: any;
  dialect: any;

  constructor(table: any, set: any, session: any, dialect: any, withList?: any[]) {
    super();
    this.session = session;
    this.dialect = dialect;
    this.config = { set, table, withList };
    this.tableName = getTableLikeName(table);
    this.joinsNotNullableMap = typeof this.tableName === 'string' ? { [this.tableName]: true } : {};
  }

  where(where: any): this {
    this.config.where = where;
    return this;
  }

  /** @internal */
  getSQL(): any {
    return this.dialect.buildUpdateQuery(this.config);
  }

  toSQL(): any {
    const { typings: _typings, ...rest } = this.dialect.sqlToQuery(this.getSQL());
    return rest;
  }

  /** @internal */
  _prepare(name?: string): any {
    const query = this.session.prepareQuery(
      this.dialect.sqlToQuery(this.getSQL()),
      undefined,
      name,
      true,
    );
    query.joinsNotNullableMap = this.joinsNotNullableMap;
    return query;
  }

  prepare(name?: string): any {
    return this._prepare(name);
  }

  override execute = (placeholderValues?: any) => {
    return this._prepare().execute(placeholderValues);
  };

  $dynamic(): this {
    return this;
  }
}
