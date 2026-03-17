import { entityKind, is } from 'drizzle-orm/entity';
import { QueryPromise } from 'drizzle-orm/query-promise';
import { Param, SQL } from 'drizzle-orm/sql/sql';
import { Table } from 'drizzle-orm/table';
// @ts-expect-error - tracer exists at runtime
import { tracer } from 'drizzle-orm/tracing';
import { haveSameKeys } from 'drizzle-orm/utils';
import { QueryBuilder } from './query-builder.ts';

const Columns = Symbol.for('drizzle:Columns');

export class SnowflakeInsertBuilder {
  static readonly [entityKind]: string = 'SnowflakeInsertBuilder';

  authToken: any;

  constructor(
    public table: any,
    public session: any,
    public dialect: any,
    public withList?: any[],
    public overridingSystemValue_?: boolean,
  ) {}

  /** @internal */
  setToken(token: any): this {
    this.authToken = token;
    return this;
  }

  values(values: any) {
    values = Array.isArray(values) ? values : [values];
    if (values.length === 0) {
      throw new Error('values() must be called with at least one value');
    }
    const mappedValues = values.map((entry: any) => {
      const result: any = {};
      const cols = this.table[(Table as any).Symbol.Columns];
      for (const colKey of Object.keys(entry)) {
        const colValue = entry[colKey];
        result[colKey] = is(colValue, SQL) ? colValue : new Param(colValue, cols[colKey]);
      }
      return result;
    });
    return new SnowflakeInsertBase(
      this.table,
      mappedValues,
      this.session,
      this.dialect,
      this.withList,
      false,
    );
  }

  select(selectQuery: any) {
    const select = typeof selectQuery === 'function' ? selectQuery(new QueryBuilder()) : selectQuery;
    if (!is(select, SQL) && !haveSameKeys(this.table[Columns], select._.selectedFields)) {
      throw new Error(
        'Insert select error: selected fields are not the same or are in a different order compared to the table definition',
      );
    }
    return new SnowflakeInsertBase(this.table, select, this.session, this.dialect, this.withList, true);
  }
}

export class SnowflakeInsertBase extends QueryPromise<any> {
  static readonly [entityKind]: string = 'SnowflakeInsert';

  config: any;
  session: any;
  dialect: any;

  constructor(table: any, values: any, session: any, dialect: any, withList?: any[], select?: boolean) {
    super();
    this.session = session;
    this.dialect = dialect;
    this.config = { table, values, withList, select };
  }

  /** @internal */
  getSQL(): SQL {
    return this.dialect.buildInsertQuery(this.config);
  }

  toSQL(): any {
    const { typings: _typings, ...rest } = this.dialect.sqlToQuery(this.getSQL());
    return rest;
  }

  /** @internal */
  _prepare(name?: string): any {
    return tracer.startActiveSpan('drizzle.prepareQuery', () => {
      return this.session.prepareQuery(
        this.dialect.sqlToQuery(this.getSQL()),
        undefined,
        name,
        true,
      );
    });
  }

  prepare(name?: string): any {
    return this._prepare(name);
  }

  override execute = (placeholderValues?: any) => {
    return tracer.startActiveSpan('drizzle.operation', () => {
      return this._prepare().execute(placeholderValues);
    });
  };

  $dynamic(): this {
    return this;
  }
}
