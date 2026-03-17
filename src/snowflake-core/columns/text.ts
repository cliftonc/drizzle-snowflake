import { entityKind } from 'drizzle-orm/entity';
import { SnowflakeColumn, SnowflakeColumnBuilder } from './common.ts';

export class SnowflakeTextBuilder extends SnowflakeColumnBuilder {
  static readonly [entityKind]: string = 'SnowflakeTextBuilder';

  constructor(name: string) {
    super(name, 'string', 'SnowflakeText');
  }

  /** @internal */
  build(table: any): SnowflakeText {
    return new SnowflakeText(table, this.config as any);
  }
}

export class SnowflakeText extends SnowflakeColumn {
  static readonly [entityKind]: string = 'SnowflakeText';

  getSQLType(): string {
    return 'text';
  }
}

export function text(name?: string) {
  return new SnowflakeTextBuilder(name ?? '');
}
