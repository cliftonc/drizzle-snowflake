import { entityKind } from 'drizzle-orm/entity';
import { QueryPromise } from 'drizzle-orm/query-promise';
// @ts-expect-error - tracer exists at runtime
import { tracer } from 'drizzle-orm/tracing';

export class SnowflakeDeleteBase extends QueryPromise<any> {
  static readonly [entityKind]: string = 'SnowflakeDelete';

  config: any;
  session: any;
  dialect: any;

  constructor(table: any, session: any, dialect: any, withList?: any[]) {
    super();
    this.session = session;
    this.dialect = dialect;
    this.config = { table, withList };
  }

  where(where: any): this {
    this.config.where = where;
    return this;
  }

  /** @internal */
  getSQL(): any {
    return this.dialect.buildDeleteQuery(this.config);
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
