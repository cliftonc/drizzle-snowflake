import { entityKind } from 'drizzle-orm/entity';
import { SnowflakeColumn, SnowflakeColumnBuilder } from './common.ts';

export class SnowflakeVarcharBuilder extends SnowflakeColumnBuilder {
  static readonly [entityKind]: string = 'SnowflakeVarcharBuilder';

  constructor(name: string, length?: number) {
    super(name, 'string', 'SnowflakeVarchar');
    (this.config as any).length = length;
  }

  /** @internal */
  build(table: any): SnowflakeVarchar {
    return new SnowflakeVarchar(table, this.config as any);
  }
}

export class SnowflakeVarchar extends SnowflakeColumn {
  static readonly [entityKind]: string = 'SnowflakeVarchar';

  readonly length: number | undefined;

  constructor(table: any, config: any) {
    super(table, config);
    this.length = config.length;
  }

  getSQLType(): string {
    return this.length ? `varchar(${this.length})` : 'varchar';
  }
}

export function varchar(name?: string, config?: { length?: number }) {
  return new SnowflakeVarcharBuilder(name ?? '', config?.length);
}
