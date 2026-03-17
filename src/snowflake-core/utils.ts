import { is } from 'drizzle-orm/entity';
import { Table } from 'drizzle-orm/table';
import { CheckBuilder } from './checks.ts';
import { ForeignKeyBuilder } from './foreign-keys.ts';
import { IndexBuilder } from './indexes.ts';
import { PrimaryKeyBuilder } from './primary-keys.ts';
import { SnowflakeTable } from './table.ts';
import { UniqueConstraintBuilder } from './unique-constraint.ts';

export function getTableConfig(table: SnowflakeTable) {
  const columns = Object.values((table as any)[(Table as any).Symbol.Columns]);
  const indexes: any[] = [];
  const checks: any[] = [];
  const primaryKeys: any[] = [];
  const foreignKeys = Object.values((table as any)[(SnowflakeTable as any).Symbol.InlineForeignKeys]);
  const uniqueConstraints: any[] = [];
  const name = (table as any)[(Table as any).Symbol.Name];
  const schema = (table as any)[(Table as any).Symbol.Schema];

  const extraConfigBuilder = (table as any)[(SnowflakeTable as any).Symbol.ExtraConfigBuilder];
  if (extraConfigBuilder !== undefined) {
    const extraConfig = extraConfigBuilder((table as any)[(Table as any).Symbol.ExtraConfigColumns]);
    const extraValues = Array.isArray(extraConfig) ? extraConfig.flat(1) : Object.values(extraConfig);
    for (const builder of extraValues) {
      if (is(builder, IndexBuilder)) {
        indexes.push(builder.build(table));
      } else if (is(builder, CheckBuilder)) {
        checks.push(builder.build(table));
      } else if (is(builder, UniqueConstraintBuilder)) {
        uniqueConstraints.push(builder.build(table));
      } else if (is(builder, PrimaryKeyBuilder)) {
        primaryKeys.push(builder.build(table));
      } else if (is(builder, ForeignKeyBuilder)) {
        foreignKeys.push(builder.build(table));
      }
    }
  }

  return {
    columns,
    indexes,
    foreignKeys,
    checks,
    primaryKeys,
    uniqueConstraints,
    name,
    schema,
  };
}
