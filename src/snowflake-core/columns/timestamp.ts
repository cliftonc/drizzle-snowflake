import { entityKind } from 'drizzle-orm/entity';
import { SnowflakeColumn } from './common.ts';
import { SnowflakeDateColumnBaseBuilder } from './date.common.ts';

export class SnowflakeTimestampBuilder extends SnowflakeDateColumnBaseBuilder {
  static readonly [entityKind]: string = 'SnowflakeTimestampBuilder';

  constructor(name: string) {
    super(name, 'date', 'SnowflakeTimestamp');
  }

  /** @internal */
  build(table: any): SnowflakeTimestamp {
    return new SnowflakeTimestamp(table, this.config as any);
  }
}

export class SnowflakeTimestamp extends SnowflakeColumn {
  static readonly [entityKind]: string = 'SnowflakeTimestamp';

  getSQLType(): string {
    return 'timestamp';
  }
}

export function timestamp(name?: string) {
  return new SnowflakeTimestampBuilder(name ?? '');
}
