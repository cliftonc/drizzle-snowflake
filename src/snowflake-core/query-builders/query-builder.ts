import { entityKind, is } from 'drizzle-orm/entity';
import { SelectionProxyHandler } from 'drizzle-orm/selection-proxy';
import { WithSubquery } from 'drizzle-orm/subquery';
import { SnowflakeDialect } from '../dialect.ts';
import { SnowflakeSelectBuilder } from './select.ts';

export class QueryBuilder {
  static readonly [entityKind]: string = 'SnowflakeQueryBuilder';

  dialect: SnowflakeDialect | undefined;
  dialectConfig: any;

  constructor(dialect?: any) {
    this.dialect = is(dialect, SnowflakeDialect) ? dialect : undefined;
    this.dialectConfig = is(dialect, SnowflakeDialect) ? undefined : dialect;
  }

  $with(alias: string) {
    const queryBuilder = this;
    return {
      as(qb: any) {
        if (typeof qb === 'function') {
          qb = qb(queryBuilder);
        }
        return new Proxy(
          new WithSubquery(qb.getSQL(), qb.getSelectedFields(), alias, true),
          new SelectionProxyHandler({
            alias,
            sqlAliasedBehavior: 'alias',
            sqlBehavior: 'error',
          }),
        );
      },
    };
  }

  with(...queries: any[]) {
    const self = this;
    function select(fields?: any) {
      return new SnowflakeSelectBuilder({
        fields: fields ?? undefined,
        session: undefined,
        dialect: self.getDialect(),
        withList: queries,
      });
    }
    function selectDistinct(fields?: any) {
      return new SnowflakeSelectBuilder({
        fields: fields ?? undefined,
        session: undefined,
        dialect: self.getDialect(),
        distinct: true,
      });
    }
    return { select, selectDistinct };
  }

  select(fields?: any) {
    return new SnowflakeSelectBuilder({
      fields: fields ?? undefined,
      session: undefined,
      dialect: this.getDialect(),
    });
  }

  selectDistinct(fields?: any) {
    return new SnowflakeSelectBuilder({
      fields: fields ?? undefined,
      session: undefined,
      dialect: this.getDialect(),
      distinct: true,
    });
  }

  // Lazy load dialect to avoid circular dependency
  private getDialect(): SnowflakeDialect {
    if (!this.dialect) {
      this.dialect = new SnowflakeDialect(this.dialectConfig);
    }
    return this.dialect;
  }
}
