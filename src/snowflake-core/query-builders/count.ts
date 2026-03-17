import { entityKind } from 'drizzle-orm/entity';
import { SQL, sql } from 'drizzle-orm/sql/sql';

export class SnowflakeCountBuilder extends SQL<number> {
  static readonly [entityKind]: string = 'SnowflakeCountBuilder';

  declare readonly [Symbol.toStringTag]: string;

  sql: SQL;
  session: any;

  constructor(params: { source: any; filters?: any; session: any }) {
    super(
      SnowflakeCountBuilder.buildEmbeddedCount(params.source, params.filters).queryChunks,
    );
    this.mapWith(Number);
    this.session = params.session;
    this.sql = SnowflakeCountBuilder.buildCount(params.source, params.filters);
  }

  static buildEmbeddedCount(source: any, filters?: any): SQL {
    return sql`(select count(*) from ${source}${sql.raw(' where ').if(filters)}${filters})`;
  }

  static buildCount(source: any, filters?: any): SQL {
    return sql`select count(*) as count from ${source}${sql.raw(' where ').if(filters)}${filters};`;
  }

  then(
    onfulfilled?: ((value: number) => any) | null,
    onrejected?: ((reason: any) => any) | null,
  ): Promise<any> {
    return Promise.resolve(this.session.count(this.sql)).then(onfulfilled, onrejected);
  }

  catch(onRejected?: ((reason: any) => any) | null): Promise<any> {
    return this.then(undefined, onRejected);
  }

  finally(onFinally?: (() => void) | null): Promise<any> {
    return this.then(
      (value: any) => {
        onFinally?.();
        return value;
      },
      (reason: any) => {
        onFinally?.();
        throw reason;
      },
    );
  }
}
