import { entityKind } from 'drizzle-orm/entity';
import { sql } from 'drizzle-orm/sql/sql';
import { SnowflakeColumnBuilder } from './common.ts';

export class SnowflakeDateColumnBaseBuilder<
  T = any,
  TRuntimeConfig extends object = object,
  TTypeConfig extends object = object,
> extends SnowflakeColumnBuilder<any, TRuntimeConfig, TTypeConfig> {
  static readonly [entityKind]: string = 'SnowflakeDateColumnBaseBuilder';

  defaultNow(): any {
    return this.default(sql`CURRENT_TIMESTAMP()`);
  }
}
