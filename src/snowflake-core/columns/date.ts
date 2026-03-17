import { entityKind } from 'drizzle-orm/entity';
import { SnowflakeColumn, SnowflakeColumnBuilder } from './common.ts';

export class SnowflakeDateBuilder extends SnowflakeColumnBuilder {
  static readonly [entityKind]: string = 'SnowflakeDateBuilder';

  constructor(name: string) {
    super(name, 'string', 'SnowflakeDate');
  }

  /** @internal */
  build(table: any): SnowflakeDate {
    return new SnowflakeDate(table, this.config as any);
  }
}

export class SnowflakeDate extends SnowflakeColumn {
  static readonly [entityKind]: string = 'SnowflakeDate';

  getSQLType(): string {
    return 'date';
  }
}

export function date(name?: string) {
  return new SnowflakeDateBuilder(name ?? '');
}
