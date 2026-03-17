import { entityKind } from 'drizzle-orm/entity';
import { SnowflakeColumn, SnowflakeColumnBuilder } from './common.ts';

export class SnowflakeRealBuilder extends SnowflakeColumnBuilder {
  static readonly [entityKind]: string = 'SnowflakeRealBuilder';

  constructor(name: string) {
    super(name, 'number', 'SnowflakeReal');
  }

  /** @internal */
  build(table: any): SnowflakeReal {
    return new SnowflakeReal(table, this.config as any);
  }
}

export class SnowflakeReal extends SnowflakeColumn {
  static readonly [entityKind]: string = 'SnowflakeReal';

  getSQLType(): string {
    return 'real';
  }
}

export function real(name?: string) {
  return new SnowflakeRealBuilder(name ?? '');
}
