import { entityKind } from 'drizzle-orm/entity';
import { SnowflakeColumn, SnowflakeColumnBuilder } from './common.ts';

export class SnowflakeBooleanBuilder extends SnowflakeColumnBuilder {
  static readonly [entityKind]: string = 'SnowflakeBooleanBuilder';

  constructor(name: string) {
    super(name, 'boolean', 'SnowflakeBoolean');
  }

  /** @internal */
  build(table: any): SnowflakeBoolean {
    return new SnowflakeBoolean(table, this.config as any);
  }
}

export class SnowflakeBoolean extends SnowflakeColumn {
  static readonly [entityKind]: string = 'SnowflakeBoolean';

  getSQLType(): string {
    return 'boolean';
  }
}

export function boolean(name?: string) {
  return new SnowflakeBooleanBuilder(name ?? '');
}
