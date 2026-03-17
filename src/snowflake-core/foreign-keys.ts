import { entityKind } from 'drizzle-orm/entity';
import type { AnySnowflakeColumn } from './columns/common.ts';
import type { SnowflakeTable } from './table.ts';

export type UpdateDeleteAction = 'cascade' | 'restrict' | 'no action' | 'set null' | 'set default';

export type Reference = () => {
  readonly columns: AnySnowflakeColumn[];
  readonly foreignColumns: AnySnowflakeColumn[];
};

export class ForeignKeyBuilder {
  static readonly [entityKind]: string = 'SnowflakeForeignKeyBuilder';

  /** @internal */
  reference: Reference;
  /** @internal */
  _onUpdate: UpdateDeleteAction | undefined = 'no action';
  /** @internal */
  _onDelete: UpdateDeleteAction | undefined = 'no action';

  constructor(config: () => { columns: AnySnowflakeColumn[]; foreignColumns: AnySnowflakeColumn[] }, actions?: { onUpdate?: UpdateDeleteAction; onDelete?: UpdateDeleteAction }) {
    this.reference = config;
    if (actions) {
      this._onUpdate = actions.onUpdate;
      this._onDelete = actions.onDelete;
    }
  }

  onUpdate(action: UpdateDeleteAction): this {
    this._onUpdate = action;
    return this;
  }

  onDelete(action: UpdateDeleteAction): this {
    this._onDelete = action;
    return this;
  }

  /** @internal */
  build(table: SnowflakeTable): ForeignKey {
    return new ForeignKey(table, this);
  }
}

export class ForeignKey {
  static readonly [entityKind]: string = 'SnowflakeForeignKey';

  readonly reference: Reference;
  readonly onUpdate: UpdateDeleteAction | undefined;
  readonly onDelete: UpdateDeleteAction | undefined;

  readonly table: SnowflakeTable;

  constructor(table: SnowflakeTable, builder: ForeignKeyBuilder) {
    this.table = table;
    this.reference = builder.reference;
    this.onUpdate = builder._onUpdate;
    this.onDelete = builder._onDelete;
  }

  getName(): string {
    const { columns, foreignColumns } = this.reference();
    const columnNames = columns.map((column) => column.name);
    const foreignColumnNames = foreignColumns.map((column) => column.name);
    const fkName = columnNames.join('_') + '_' + foreignColumnNames.join('_') + '_fk';
    return fkName;
  }
}

export function foreignKey(config: {
  columns: AnySnowflakeColumn[];
  foreignColumns: AnySnowflakeColumn[];
  name?: string;
}): ForeignKeyBuilder {
  return new ForeignKeyBuilder(() => ({
    columns: config.columns,
    foreignColumns: config.foreignColumns,
  }));
}
