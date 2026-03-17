import { entityKind } from 'drizzle-orm/entity';
import { Table } from 'drizzle-orm/table';
import type { AnySnowflakeColumn } from './columns/common.ts';
import type { SnowflakeTable } from './table.ts';

export function uniqueKeyName(table: SnowflakeTable, columns: string[]): string {
  return `${(table as any)[(Table as any).Symbol.Name]}_${columns.join('_')}_unique`;
}

export class UniqueConstraintBuilder {
  static readonly [entityKind]: string = 'SnowflakeUniqueConstraintBuilder';

  /** @internal */
  columns: AnySnowflakeColumn[];
  /** @internal */
  nullsNotDistinct = false;
  /** @internal */
  name?: string;

  constructor(columns: AnySnowflakeColumn[], name?: string) {
    this.columns = columns;
    this.name = name;
  }

  nullsDistinct(): this {
    this.nullsNotDistinct = false;
    return this;
  }

  /** @internal */
  build(table: SnowflakeTable): UniqueConstraint {
    return new UniqueConstraint(table, this.columns, this.nullsNotDistinct, this.name);
  }
}

export class UniqueConstraint {
  static readonly [entityKind]: string = 'SnowflakeUniqueConstraint';

  readonly columns: AnySnowflakeColumn[];
  readonly name?: string;
  readonly nullsNotDistinct: boolean;

  constructor(
    readonly table: SnowflakeTable,
    columns: AnySnowflakeColumn[],
    nullsNotDistinct: boolean,
    name?: string,
  ) {
    this.columns = columns;
    this.name = name ?? uniqueKeyName(table, columns.map((c) => c.name));
    this.nullsNotDistinct = nullsNotDistinct;
  }
}

export function unique(name?: string): { on: (...columns: AnySnowflakeColumn[]) => UniqueConstraintBuilder } {
  return {
    on: (...columns: AnySnowflakeColumn[]) => new UniqueConstraintBuilder(columns, name),
  };
}
