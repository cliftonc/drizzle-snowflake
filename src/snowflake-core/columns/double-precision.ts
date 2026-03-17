import { entityKind } from 'drizzle-orm/entity';
import { SnowflakeColumn, SnowflakeColumnBuilder } from './common.ts';

export class SnowflakeDoublePrecisionBuilder extends SnowflakeColumnBuilder {
  static readonly [entityKind]: string = 'SnowflakeDoublePrecisionBuilder';

  constructor(name: string) {
    super(name, 'number', 'SnowflakeDoublePrecision');
  }

  /** @internal */
  build(table: any): SnowflakeDoublePrecision {
    return new SnowflakeDoublePrecision(table, this.config as any);
  }
}

export class SnowflakeDoublePrecision extends SnowflakeColumn {
  static readonly [entityKind]: string = 'SnowflakeDoublePrecision';

  getSQLType(): string {
    return 'double precision';
  }

  mapFromDriverValue(value: any): number {
    if (typeof value === 'string') {
      return Number.parseFloat(value);
    }
    return value;
  }
}

export function doublePrecision(name?: string) {
  return new SnowflakeDoublePrecisionBuilder(name ?? '');
}
