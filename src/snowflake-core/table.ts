import type { BuildColumns, BuildExtraConfigColumns } from 'drizzle-orm/column-builder';
import { entityKind } from 'drizzle-orm/entity';
import { Table, type TableConfig as TableConfigBase, type UpdateTableConfig } from 'drizzle-orm/table';
import type { CheckBuilder } from './checks.ts';
import type { SnowflakeColumnsBuilders } from './columns/all.ts';
import { getSnowflakeColumnBuilders } from './columns/all.ts';
import type { SnowflakeColumn, SnowflakeColumnBuilder } from './columns/common.ts';
import type { ForeignKeyBuilder } from './foreign-keys.ts';
import type { AnyIndexBuilder } from './indexes.ts';
import type { PrimaryKeyBuilder } from './primary-keys.ts';
import type { UniqueConstraintBuilder } from './unique-constraint.ts';

export type SnowflakeTableExtraConfigValue =
  | AnyIndexBuilder
  | CheckBuilder
  | ForeignKeyBuilder
  | PrimaryKeyBuilder
  | UniqueConstraintBuilder;

export type SnowflakeTableExtraConfig = Record<string, SnowflakeTableExtraConfigValue>;

export type TableConfig = TableConfigBase<SnowflakeColumn>;

const InlineForeignKeys = Symbol.for('drizzle:SnowflakeInlineForeignKeys');
const ExtraConfigBuilder = (Table as any).Symbol?.ExtraConfigBuilder ?? Symbol.for('drizzle:ExtraConfigBuilder');
const ExtraConfigColumns = (Table as any).Symbol?.ExtraConfigColumns ?? Symbol.for('drizzle:ExtraConfigColumns');

export type SnowflakeColumnBuilderBase = import('./columns/common.ts').SnowflakeColumnBuilder;

export class SnowflakeTable<T extends TableConfig = TableConfig> extends Table<T> {
  static readonly [entityKind]: string = 'SnowflakeTable';

  /** @internal */
  static Symbol = Object.assign({}, (Table as any).Symbol, {
    InlineForeignKeys,
  });

  /** @internal */
  [InlineForeignKeys]: any[] = [];

  /** @internal */
  [ExtraConfigBuilder]: any = undefined;

  /** @internal */
  [ExtraConfigColumns]: any = {};
}

export type AnySnowflakeTable<TPartial extends Partial<TableConfig> = {}> = SnowflakeTable<
  UpdateTableConfig<TableConfig, TPartial>
>;

export type SnowflakeTableWithColumns<T extends TableConfig> = SnowflakeTable<T> & {
  [Key in keyof T['columns']]: T['columns'][Key];
};

function snowflakeTableWithSchema(
  name: string,
  columns: Record<string, any> | ((columnTypes: SnowflakeColumnsBuilders) => Record<string, any>),
  extraConfig: ((self: any) => any) | undefined,
  schema: string | undefined,
  baseName: string = name,
) {
  const rawTable = new SnowflakeTable(name, schema, baseName);

  const parsedColumns: Record<string, any> =
    typeof columns === 'function' ? columns(getSnowflakeColumnBuilders()) : columns;

  const builtColumns = Object.fromEntries(
    Object.entries(parsedColumns).map(([name2, colBuilderBase]) => {
      const colBuilder = colBuilderBase as any;
      colBuilder.setName(name2);
      const column = colBuilder.build(rawTable);
      (rawTable as any)[InlineForeignKeys].push(...colBuilder.buildForeignKeys(column, rawTable));
      return [name2, column];
    }),
  );

  const builtColumnsForExtraConfig = Object.fromEntries(
    Object.entries(parsedColumns).map(([name2, colBuilderBase]) => {
      const colBuilder = colBuilderBase as any;
      colBuilder.setName(name2);
      const column = colBuilder.buildExtraConfigColumn(rawTable);
      return [name2, column];
    }),
  );

  const table = Object.assign(rawTable, builtColumns) as any;
  table[(Table as any).Symbol.Columns] = builtColumns;
  table[(Table as any).Symbol.ExtraConfigColumns] = builtColumnsForExtraConfig;

  if (extraConfig) {
    table[(SnowflakeTable as any).Symbol.ExtraConfigBuilder] = extraConfig;
  }

  return table;
}

export const snowflakeTable = (
  name: string,
  columns: Record<string, any> | ((columnTypes: SnowflakeColumnsBuilders) => Record<string, any>),
  extraConfig?: (self: any) => any,
) => {
  return snowflakeTableWithSchema(name, columns, extraConfig, undefined);
};

export function snowflakeTableCreator(customizeTableName: (name: string) => string) {
  return (
    name: string,
    columns: Record<string, any> | ((columnTypes: SnowflakeColumnsBuilders) => Record<string, any>),
    extraConfig?: (self: any) => any,
  ) => {
    return snowflakeTableWithSchema(customizeTableName(name), columns, extraConfig, undefined, name);
  };
}

export { snowflakeTableWithSchema };
