import { entityKind } from 'drizzle-orm/entity';
import { SnowflakeColumn } from './common.ts';
import { SnowflakeDateColumnBaseBuilder } from './date.common.ts';

export class SnowflakeTimestampBuilder extends SnowflakeDateColumnBaseBuilder {
  static readonly [entityKind]: string = 'SnowflakeTimestampBuilder';

  constructor(name: string, mode: 'date' | 'string') {
    super(name, mode, 'SnowflakeTimestamp');
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

export function timestamp(name?: string, config?: { mode?: 'date' | 'string' }) {
  return new SnowflakeTimestampBuilder(name ?? '', config?.mode ?? 'date');
}
