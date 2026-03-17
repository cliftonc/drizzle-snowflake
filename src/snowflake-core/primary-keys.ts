import { entityKind } from 'drizzle-orm/entity';
import type { AnySnowflakeColumn } from './columns/common.ts';
import { SnowflakeTable } from './table.ts';

export function primaryKey(...config: any[]): PrimaryKeyBuilder {
  if (config[0].columns) {
    return new PrimaryKeyBuilder(config[0].columns, config[0].name);
  }
  return new PrimaryKeyBuilder(config);
}

export class PrimaryKeyBuilder {
  static readonly [entityKind]: string = 'SnowflakePrimaryKeyBuilder';

  /** @internal */
  columns: AnySnowflakeColumn[];
  /** @internal */
  name?: string;

  constructor(columns: AnySnowflakeColumn[], name?: string) {
    this.columns = columns;
    this.name = name;
  }

  /** @internal */
  build(table: SnowflakeTable): PrimaryKey {
    return new PrimaryKey(table, this.columns, this.name);
  }
}

export class PrimaryKey {
  static readonly [entityKind]: string = 'SnowflakePrimaryKey';

  readonly columns: AnySnowflakeColumn[];
  readonly name?: string;

  constructor(
    readonly table: SnowflakeTable,
    columns: AnySnowflakeColumn[],
    name?: string,
  ) {
    this.columns = columns;
    this.name = name;
  }

  getName(): string {
    return (
      this.name ??
      `${(this.table as any)[(SnowflakeTable as any).Symbol.Name]}_${this.columns.map((column) => column.name).join('_')}_pk`
    );
  }
}
