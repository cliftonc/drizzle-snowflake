import { entityKind } from 'drizzle-orm/entity';
import { SnowflakeColumn, SnowflakeColumnBuilder } from './common.ts';

export class SnowflakeDecimalBuilder extends SnowflakeColumnBuilder {
  static readonly [entityKind]: string = 'SnowflakeDecimalBuilder';

  constructor(name: string) {
    super(name, 'string', 'SnowflakeDecimal');
  }

  /** @internal */
  build(table: any): SnowflakeDecimal {
    return new SnowflakeDecimal(table, this.config as any);
  }
}

export class SnowflakeDecimal extends SnowflakeColumn {
  static readonly [entityKind]: string = 'SnowflakeDecimal';

  getSQLType(): string {
    return 'numeric';
  }
}

export function decimal(name?: string) {
  return new SnowflakeDecimalBuilder(name ?? '');
}
