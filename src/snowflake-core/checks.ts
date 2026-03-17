import { entityKind } from 'drizzle-orm/entity';
import type { SQL } from 'drizzle-orm/sql/sql';
import type { SnowflakeTable } from './table.ts';

export class CheckBuilder {
  static readonly [entityKind]: string = 'SnowflakeCheckBuilder';

  protected brand!: 'SnowflakeCheckBuilder';
  readonly name: string;
  readonly value: SQL;

  constructor(name: string, value: SQL) {
    this.name = name;
    this.value = value;
  }

  /** @internal */
  build(table: SnowflakeTable): Check {
    return new Check(table, this);
  }
}

export class Check {
  static readonly [entityKind]: string = 'SnowflakeCheck';

  readonly name: string;
  readonly value: SQL;

  readonly table: SnowflakeTable;

  constructor(table: SnowflakeTable, builder: CheckBuilder) {
    this.table = table;
    this.name = builder.name;
    this.value = builder.value;
  }
}

export function check(name: string, value: SQL): CheckBuilder {
  return new CheckBuilder(name, value);
}
