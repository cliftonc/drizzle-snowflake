import { entityKind } from 'drizzle-orm/entity';
import { SnowflakeColumn, SnowflakeColumnBuilder } from './common.ts';

export class SnowflakeJsonBuilder extends SnowflakeColumnBuilder {
  static readonly [entityKind]: string = 'SnowflakeJsonBuilder';

  constructor(name: string) {
    super(name, 'json', 'SnowflakeJson');
  }

  /** @internal */
  build(table: any): SnowflakeJson {
    return new SnowflakeJson(table, this.config as any);
  }
}

export class SnowflakeJson extends SnowflakeColumn {
  static readonly [entityKind]: string = 'SnowflakeJson';

  getSQLType(): string {
    return 'variant';
  }
}

export function json(name?: string) {
  return new SnowflakeJsonBuilder(name ?? '');
}
