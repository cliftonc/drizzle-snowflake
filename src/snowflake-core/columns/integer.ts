import { entityKind } from 'drizzle-orm/entity';
import { SnowflakeColumn } from './common.ts';
import { SnowflakeIntColumnBaseBuilder } from './int.common.ts';

export class SnowflakeIntegerBuilder extends SnowflakeIntColumnBaseBuilder {
  static readonly [entityKind]: string = 'SnowflakeIntegerBuilder';

  constructor(name: string) {
    super(name, 'number', 'SnowflakeInteger');
  }

  /** @internal */
  build(table: any): SnowflakeInteger {
    return new SnowflakeInteger(table, this.config as any);
  }
}

export class SnowflakeInteger extends SnowflakeColumn {
  static readonly [entityKind]: string = 'SnowflakeInteger';

  getSQLType(): string {
    return 'integer';
  }
}

export function integer(name?: string) {
  return new SnowflakeIntegerBuilder(name ?? '');
}
