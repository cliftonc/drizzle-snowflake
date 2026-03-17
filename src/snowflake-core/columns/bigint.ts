import { entityKind } from 'drizzle-orm/entity';
import { SnowflakeColumn } from './common.ts';
import { SnowflakeIntColumnBaseBuilder } from './int.common.ts';

export class SnowflakeBigIntBuilder extends SnowflakeIntColumnBaseBuilder {
  static readonly [entityKind]: string = 'SnowflakeBigIntBuilder';

  constructor(name: string) {
    super(name, 'number', 'SnowflakeBigInt');
  }

  /** @internal */
  build(table: any): SnowflakeBigInt {
    return new SnowflakeBigInt(table, this.config as any);
  }
}

export class SnowflakeBigInt extends SnowflakeColumn {
  static readonly [entityKind]: string = 'SnowflakeBigInt';

  getSQLType(): string {
    return 'bigint';
  }
}

export function bigint(name?: string) {
  return new SnowflakeBigIntBuilder(name ?? '');
}
