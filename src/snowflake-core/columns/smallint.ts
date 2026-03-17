import { entityKind } from 'drizzle-orm/entity';
import { SnowflakeColumn } from './common.ts';
import { SnowflakeIntColumnBaseBuilder } from './int.common.ts';

export class SnowflakeSmallIntBuilder extends SnowflakeIntColumnBaseBuilder {
  static readonly [entityKind]: string = 'SnowflakeSmallIntBuilder';

  constructor(name: string) {
    super(name, 'number', 'SnowflakeSmallInt');
  }

  /** @internal */
  build(table: any): SnowflakeSmallInt {
    return new SnowflakeSmallInt(table, this.config as any);
  }
}

export class SnowflakeSmallInt extends SnowflakeColumn {
  static readonly [entityKind]: string = 'SnowflakeSmallInt';

  getSQLType(): string {
    return 'smallint';
  }
}

export function smallint(name?: string) {
  return new SnowflakeSmallIntBuilder(name ?? '');
}
