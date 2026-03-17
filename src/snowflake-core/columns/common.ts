import type { ColumnBaseConfig } from 'drizzle-orm/column';
import { Column } from 'drizzle-orm/column';
import type {
  ColumnBuilderBaseConfig,
  ColumnBuilderRuntimeConfig,
  ColumnDataType,
  HasGenerated,
} from 'drizzle-orm/column-builder';
import { ColumnBuilder } from 'drizzle-orm/column-builder';
import { entityKind } from 'drizzle-orm/entity';
import type { SQL } from 'drizzle-orm/sql/sql';
import { iife } from 'drizzle-orm/tracing-utils';
import type { Update } from 'drizzle-orm/utils';
import type { UpdateDeleteAction } from '../foreign-keys.ts';
import { ForeignKeyBuilder } from '../foreign-keys.ts';
import type { AnySnowflakeTable, SnowflakeTable } from '../table.ts';
import { uniqueKeyName } from '../unique-constraint.ts';

export interface ReferenceConfig {
  ref: () => SnowflakeColumn;
  actions: {
    onUpdate?: UpdateDeleteAction;
    onDelete?: UpdateDeleteAction;
  };
}

export abstract class SnowflakeColumnBuilder<
  T extends ColumnBuilderBaseConfig<ColumnDataType, string> = ColumnBuilderBaseConfig<ColumnDataType, string>,
  TRuntimeConfig extends object = object,
  TTypeConfig extends object = object,
> extends ColumnBuilder<T, TRuntimeConfig, TTypeConfig & { dialect: 'snowflake' }> {
  private foreignKeyConfigs: ReferenceConfig[] = [];

  static readonly [entityKind]: string = 'SnowflakeColumnBuilder';

  references(ref: ReferenceConfig['ref'], actions: ReferenceConfig['actions'] = {}): this {
    this.foreignKeyConfigs.push({ ref, actions });
    return this;
  }

  unique(name?: string, config?: { nulls: 'distinct' | 'not distinct' }): this {
    this.config.isUnique = true;
    this.config.uniqueName = name;
    this.config.uniqueType = config?.nulls;
    return this;
  }

  generatedAlwaysAs(as: SQL | T['data'] | (() => SQL)): HasGenerated<this, { type: 'always' }> {
    this.config.generated = {
      as,
      type: 'always',
      mode: 'stored',
    };
    return this as any;
  }

  /** @internal */
  buildForeignKeys(column: SnowflakeColumn, table: SnowflakeTable): any[] {
    return this.foreignKeyConfigs.map(({ ref, actions }) => {
      return iife(
        (ref2: any, actions2: any) => {
          const builder = new ForeignKeyBuilder(() => {
            const foreignColumn = ref2();
            return { columns: [column], foreignColumns: [foreignColumn] };
          });
          if (actions2.onUpdate) {
            builder.onUpdate(actions2.onUpdate);
          }
          if (actions2.onDelete) {
            builder.onDelete(actions2.onDelete);
          }
          return builder.build(table);
        },
        ref,
        actions,
      );
    });
  }

  /** @internal */
  buildExtraConfigColumn(table: SnowflakeTable): SnowflakeExtraConfigColumn {
    return new SnowflakeExtraConfigColumn(table, this.config as any);
  }
}

export abstract class SnowflakeColumn<
  T extends ColumnBaseConfig<ColumnDataType, string> = ColumnBaseConfig<ColumnDataType, string>,
  TRuntimeConfig extends object = {},
  TTypeConfig extends object = {},
> extends Column<T, TRuntimeConfig, TTypeConfig & { dialect: 'snowflake' }> {
  declare readonly table: SnowflakeTable;

  static readonly [entityKind]: string = 'SnowflakeColumn';

  constructor(table: SnowflakeTable, config: ColumnBuilderRuntimeConfig<T['data'], TRuntimeConfig>) {
    if (!config.uniqueName) {
      config.uniqueName = uniqueKeyName(table, [config.name]);
    }
    super(table, config);
    this.table = table;
  }
}

export type IndexedExtraConfigType = {
  order?: 'asc' | 'desc';
  nulls?: 'first' | 'last';
  opClass?: string;
};

export class SnowflakeExtraConfigColumn<
  T extends ColumnBaseConfig<ColumnDataType, string> = ColumnBaseConfig<ColumnDataType, string>,
> extends SnowflakeColumn<T, IndexedExtraConfigType> {
  static readonly [entityKind]: string = 'SnowflakeExtraConfigColumn';

  getSQLType(): string {
    return this.getSQLType();
  }

  indexConfig: IndexedExtraConfigType = {
    order: (this.config as any).order ?? 'asc',
    nulls: (this.config as any).nulls ?? 'last',
    opClass: (this.config as any).opClass,
  };

  defaultConfig: IndexedExtraConfigType = {
    order: 'asc',
    nulls: 'last',
    opClass: undefined,
  };

  asc(): Omit<this, 'asc' | 'desc'> {
    this.indexConfig.order = 'asc';
    return this as any;
  }

  desc(): Omit<this, 'asc' | 'desc'> {
    this.indexConfig.order = 'desc';
    return this as any;
  }

  nullsFirst(): Omit<this, 'nullsFirst' | 'nullsLast'> {
    this.indexConfig.nulls = 'first';
    return this as any;
  }

  nullsLast(): Omit<this, 'nullsFirst' | 'nullsLast'> {
    this.indexConfig.nulls = 'last';
    return this as any;
  }

  op(opClass: string): Omit<this, 'op'> {
    this.indexConfig.opClass = opClass;
    return this as any;
  }
}

export class IndexedColumn {
  static readonly [entityKind]: string = 'IndexedColumn';

  name: string | undefined;
  keyAsName: boolean;
  type: string;
  indexConfig: IndexedExtraConfigType;

  constructor(
    name: string | undefined,
    keyAsName: boolean,
    type: string,
    indexConfig: IndexedExtraConfigType,
  ) {
    this.name = name;
    this.keyAsName = keyAsName;
    this.type = type;
    this.indexConfig = indexConfig;
  }
}

export type AnySnowflakeColumn<TPartial extends Partial<ColumnBaseConfig<ColumnDataType, string>> = {}> =
  SnowflakeColumn<Required<Update<ColumnBaseConfig<ColumnDataType, string>, TPartial>>>;
