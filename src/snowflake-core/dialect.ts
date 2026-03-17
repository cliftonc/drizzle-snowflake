import { CasingCache } from 'drizzle-orm/casing';
import { Column } from 'drizzle-orm/column';
import { entityKind, is } from 'drizzle-orm/entity';
import type { MigrationConfig, MigrationMeta } from 'drizzle-orm/migrator';
import { Param, SQL, sql, View } from 'drizzle-orm/sql/sql';
import { Subquery } from 'drizzle-orm/subquery';
import { getTableName, Table } from 'drizzle-orm/table';
// @ts-expect-error - orderSelectedFields exists at runtime
import { orderSelectedFields } from 'drizzle-orm/utils';
import { ViewBaseConfig } from 'drizzle-orm/view-common';
import { SnowflakeColumn } from './columns/common.ts';
import { SnowflakeDecimal } from './columns/decimal.ts';
import { SnowflakeJson } from './columns/json.ts';
import { SnowflakeTimestamp } from './columns/timestamp.ts';
import type { SnowflakeSession } from './session.ts';
import { SnowflakeTable } from './table.ts';
import { SnowflakeViewBase } from './view-base.ts';

export class SnowflakeDialect {
  static readonly [entityKind]: string = 'SnowflakeDialect';

  /** @internal */
  casing: any;

  constructor(config?: { casing?: any }) {
    this.casing = new CasingCache(config?.casing);
  }

  escapeName(name: string): string {
    // Snowflake uses unquoted identifiers that resolve to uppercase.
    // Quoting preserves case, which causes mismatches between Drizzle's
    // quoted references and external code using sql.raw() (e.g. CTE names).
    // Not quoting keeps everything case-insensitive and consistent.
    return name;
  }

  escapeParam(_num: number): string {
    return '?';
  }

  escapeString(str: string): string {
    return `'${str.replace(/'/g, "''")}'`;
  }

  async migrate(
    migrations: MigrationMeta[],
    session: SnowflakeSession<any, any>,
    config: MigrationConfig | string,
  ): Promise<void> {
    const migrationConfig: MigrationConfig =
      typeof config === 'string' ? { migrationsFolder: config } : config;

    const migrationsSchema = migrationConfig.migrationsSchema ?? 'PUBLIC';
    const migrationsTable = migrationConfig.migrationsTable ?? '__drizzle_migrations';

    const migrationTableCreate = sql`
      CREATE TABLE IF NOT EXISTS ${sql.identifier(migrationsSchema)}.${sql.identifier(
        migrationsTable,
      )} (
        id INT NOT NULL,
        hash VARCHAR NOT NULL,
        created_at BIGINT
      )
    `;

    await session.execute(migrationTableCreate);

    const dbMigrations = await session.all<{
      id: number;
      hash: string;
      created_at: string;
    }>(
      sql`SELECT id, hash, created_at FROM ${sql.identifier(
        migrationsSchema,
      )}.${sql.identifier(migrationsTable)} ORDER BY created_at DESC LIMIT 1`,
    );

    const lastDbMigration = dbMigrations[0];

    await session.transaction(async (tx: any) => {
      for await (const migration of migrations) {
        if (
          !lastDbMigration ||
          Number(lastDbMigration.created_at) < migration.folderMillis
        ) {
          for (const stmt of migration.sql) {
            await tx.execute(sql.raw(stmt));
          }

          await tx.execute(
            sql`INSERT INTO ${sql.identifier(
              migrationsSchema,
            )}.${sql.identifier(migrationsTable)} (id, hash, created_at)
              VALUES (
                (SELECT COALESCE(MAX(id), 0) + 1 FROM ${sql.identifier(
                  migrationsSchema,
                )}.${sql.identifier(migrationsTable)}),
                ${migration.hash},
                ${migration.folderMillis}
              )`,
          );
        }
      }
    });
  }

  buildWithCTE(queries: any[] | undefined): SQL | undefined {
    if (!queries?.length) return undefined;
    const withSqlChunks = [sql`with `];
    for (const [i, w] of queries.entries()) {
      withSqlChunks.push(sql`${sql.identifier(w._.alias)} as (${w._.sql})`);
      if (i < queries.length - 1) {
        withSqlChunks.push(sql`, `);
      }
    }
    withSqlChunks.push(sql` `);
    return sql.join(withSqlChunks);
  }

  buildDeleteQuery({ table, where, withList }: any): SQL {
    const withSql = this.buildWithCTE(withList);
    const whereSql = where ? sql` where ${where}` : undefined;
    return sql`${withSql}delete from ${table}${whereSql}`;
  }

  buildUpdateSet(table: any, set: any): SQL {
    const tableColumns = table[(Table as any).Symbol.Columns];
    const columnNames = Object.keys(tableColumns).filter(
      (colName) => set[colName] !== undefined || tableColumns[colName]?.onUpdateFn !== undefined,
    );
    const setSize = columnNames.length;
    return sql.join(
      columnNames.flatMap((colName, i) => {
        const col = tableColumns[colName];
        const value = set[colName] ?? sql.param(col.onUpdateFn(), col);
        const res = sql`${sql.identifier(this.casing.getColumnCasing(col))} = ${value}`;
        if (i < setSize - 1) {
          return [res, sql.raw(', ')];
        }
        return [res];
      }),
    );
  }

  buildUpdateQuery({ table, set, where, withList }: any): SQL {
    const withSql = this.buildWithCTE(withList);
    const tableName = table[(SnowflakeTable as any).Symbol.Name];
    const tableSchema = table[(SnowflakeTable as any).Symbol.Schema];
    const origTableName = table[(SnowflakeTable as any).Symbol.OriginalName];
    const alias = tableName === origTableName ? undefined : tableName;
    const tableSql = sql`${tableSchema ? sql`${sql.identifier(tableSchema)}.` : undefined}${sql.identifier(origTableName)}${alias && sql` ${sql.identifier(alias)}`}`;
    const setSql = this.buildUpdateSet(table, set);
    const whereSql = where ? sql` where ${where}` : undefined;
    return sql`${withSql}update ${tableSql} set ${setSql}${whereSql}`;
  }

  buildSelection(fields: any[], { isSingleTable = false } = {}): SQL {
    const columnsLen = fields.length;
    const chunks = fields.flatMap(({ field }: any, i: number) => {
      const chunk: any[] = [];
      if (is(field, SQL.Aliased) && (field as any).isSelectionField) {
        chunk.push(sql.identifier(field.fieldAlias));
      } else if (is(field, SQL.Aliased) || is(field, SQL)) {
        const query = is(field, SQL.Aliased) ? field.sql : field;
        if (isSingleTable) {
          chunk.push(
            new SQL(
              query.queryChunks.map((c: any) => {
                if (is(c, SnowflakeColumn)) {
                  return sql.identifier(this.casing.getColumnCasing(c));
                }
                return c;
              }),
            ),
          );
        } else {
          chunk.push(query);
        }
        if (is(field, SQL.Aliased)) {
          chunk.push(sql` as ${sql.identifier(field.fieldAlias)}`);
        }
      } else if (is(field, Column)) {
        if (isSingleTable) {
          chunk.push(sql.identifier(this.casing.getColumnCasing(field)));
        } else {
          chunk.push(field);
        }
      }
      if (i < columnsLen - 1) {
        chunk.push(sql`, `);
      }
      return chunk;
    });
    return sql.join(chunks);
  }

  buildJoins(joins: any[] | undefined): SQL | undefined {
    if (!joins || joins.length === 0) {
      return undefined;
    }
    const joinsArray: SQL[] = [];
    for (const [index, joinMeta] of joins.entries()) {
      if (index === 0) {
        joinsArray.push(sql` `);
      }
      const table = joinMeta.table;
      const isCross = joinMeta.joinType === 'cross';
      const lateralSql = joinMeta.lateral ? sql` lateral` : undefined;
      const onClause = isCross && !joinMeta.on ? undefined : joinMeta.on ? sql` on ${joinMeta.on}` : undefined;
      if (is(table, SnowflakeTable)) {
        const tableName = table[(SnowflakeTable as any).Symbol.Name];
        const tableSchema = table[(SnowflakeTable as any).Symbol.Schema];
        const origTableName = table[(SnowflakeTable as any).Symbol.OriginalName];
        const alias = tableName === origTableName ? undefined : joinMeta.alias;
        joinsArray.push(
          sql`${sql.raw(joinMeta.joinType)} join${lateralSql} ${tableSchema ? sql`${sql.identifier(tableSchema)}.` : undefined}${sql.identifier(origTableName)}${alias && sql` ${sql.identifier(alias)}`}${onClause}`,
        );
      } else if (is(table, View)) {
        const viewName = (table as any)[ViewBaseConfig].name;
        const viewSchema = (table as any)[ViewBaseConfig].schema;
        const origViewName = (table as any)[ViewBaseConfig].originalName;
        const alias = viewName === origViewName ? undefined : joinMeta.alias;
        joinsArray.push(
          sql`${sql.raw(joinMeta.joinType)} join${lateralSql} ${viewSchema ? sql`${sql.identifier(viewSchema)}.` : undefined}${sql.identifier(origViewName)}${alias && sql` ${sql.identifier(alias)}`}${onClause}`,
        );
      } else {
        joinsArray.push(
          sql`${sql.raw(joinMeta.joinType)} join${lateralSql} ${table}${onClause}`,
        );
      }
      if (index < joins.length - 1) {
        joinsArray.push(sql` `);
      }
    }
    return sql.join(joinsArray);
  }

  buildFromTable(table: any): any {
    if (is(table, Table) && (table as any)[(Table as any).Symbol.OriginalName] !== (table as any)[(Table as any).Symbol.Name]) {
      let fullName = sql`${sql.identifier((table as any)[(Table as any).Symbol.OriginalName])}`;
      if ((table as any)[(Table as any).Symbol.Schema]) {
        fullName = sql`${sql.identifier((table as any)[(Table as any).Symbol.Schema])}.${fullName}`;
      }
      return sql`${fullName} ${sql.identifier((table as any)[(Table as any).Symbol.Name])}`;
    }
    return table;
  }

  buildSelectQuery({
    withList,
    fields,
    fieldsFlat,
    where,
    having,
    table,
    joins,
    orderBy,
    groupBy,
    limit,
    offset,
    distinct,
    setOperators,
  }: any): SQL {
    const fieldsList = fieldsFlat ?? orderSelectedFields(fields);

    for (const f of fieldsList) {
      if (
        is(f.field, Column) &&
        getTableName(f.field.table) !==
          (is(table, Subquery)
            ? table._.alias
            : is(table, SnowflakeViewBase)
              ? (table as any)[ViewBaseConfig].name
              : is(table, SQL)
                ? undefined
                : getTableName(table)) &&
        !((table2: any) =>
          joins?.some(
            ({ alias }: any) =>
              alias ===
              (table2[(Table as any).Symbol.IsAlias] ? getTableName(table2) : table2[(Table as any).Symbol.BaseName]),
          ))(f.field.table)
      ) {
        const tableName = getTableName(f.field.table);
        throw new Error(
          `Your "${f.path.join('->')}" field references a column "${tableName}"."${f.field.name}", but the table "${tableName}" is not part of the query! Did you forget to join it?`,
        );
      }
    }

    const isSingleTable = !joins || joins.length === 0;
    const withSql = this.buildWithCTE(withList);

    let distinctSql: SQL | undefined;
    if (distinct) {
      distinctSql =
        distinct === true
          ? sql` distinct`
          : sql` distinct on (${sql.join(distinct.on, sql`, `)})`;
    }

    const selection = this.buildSelection(fieldsList, { isSingleTable });
    const tableSql = this.buildFromTable(table);
    const joinsSql = this.buildJoins(joins);
    const whereSql = where ? sql` where ${where}` : undefined;
    const havingSql = having ? sql` having ${having}` : undefined;

    let orderBySql: SQL | undefined;
    if (orderBy && orderBy.length > 0) {
      orderBySql = sql` order by ${sql.join(orderBy, sql`, `)}`;
    }

    let groupBySql: SQL | undefined;
    if (groupBy && groupBy.length > 0) {
      groupBySql = sql` group by ${sql.join(groupBy, sql`, `)}`;
    }

    const limitSql =
      typeof limit === 'object' || (typeof limit === 'number' && limit >= 0)
        ? sql` limit ${limit}`
        : undefined;
    const offsetSql = offset ? sql` offset ${offset}` : undefined;

    const finalQuery = sql`${withSql}select${distinctSql} ${selection} from ${tableSql}${joinsSql}${whereSql}${groupBySql}${havingSql}${orderBySql}${limitSql}${offsetSql}`;

    if (setOperators.length > 0) {
      return this.buildSetOperations(finalQuery, setOperators);
    }
    return finalQuery;
  }

  buildSetOperations(leftSelect: SQL, setOperators: any[]): SQL {
    const [setOperator, ...rest] = setOperators;
    if (!setOperator) {
      throw new Error('Cannot pass undefined values to any set operator');
    }
    if (rest.length === 0) {
      return this.buildSetOperationQuery({ leftSelect, setOperator });
    }
    return this.buildSetOperations(
      this.buildSetOperationQuery({ leftSelect, setOperator }),
      rest,
    );
  }

  buildSetOperationQuery({
    leftSelect,
    setOperator: { type, isAll, rightSelect, limit, orderBy, offset },
  }: any): SQL {
    const leftChunk = sql`(${leftSelect.getSQL ? leftSelect.getSQL() : leftSelect}) `;
    const rightChunk = sql`(${rightSelect.getSQL()})`;

    let orderBySql: SQL | undefined;
    if (orderBy && orderBy.length > 0) {
      const orderByValues: any[] = [];
      for (const singleOrderBy of orderBy) {
        if (is(singleOrderBy, SnowflakeColumn)) {
          orderByValues.push(sql.identifier(singleOrderBy.name));
        } else if (is(singleOrderBy, SQL)) {
          for (let i = 0; i < singleOrderBy.queryChunks.length; i++) {
            const chunk = singleOrderBy.queryChunks[i];
            if (is(chunk, SnowflakeColumn)) {
              singleOrderBy.queryChunks[i] = sql.identifier(chunk.name);
            }
          }
          orderByValues.push(sql`${singleOrderBy}`);
        } else {
          orderByValues.push(sql`${singleOrderBy}`);
        }
      }
      orderBySql = sql` order by ${sql.join(orderByValues, sql`, `)} `;
    }

    const limitSql =
      typeof limit === 'object' || (typeof limit === 'number' && limit >= 0)
        ? sql` limit ${limit}`
        : undefined;
    const operatorChunk = sql.raw(`${type} ${isAll ? 'all ' : ''}`);
    const offsetSql = offset ? sql` offset ${offset}` : undefined;
    return sql`${leftChunk}${operatorChunk}${rightChunk}${orderBySql}${limitSql}${offsetSql}`;
  }

  buildInsertQuery({ table, values: valuesOrSelect, withList, select, overridingSystemValue_ }: any): SQL {
    const valuesSqlList: any[] = [];
    const columns = table[(Table as any).Symbol.Columns];
    const colEntries = Object.entries(columns).filter(
      ([_, col]: any) => !col.shouldDisableInsert(),
    );
    const insertOrder = colEntries.map(([, column]: any) =>
      sql.identifier(this.casing.getColumnCasing(column)),
    );

    if (select) {
      const select2 = valuesOrSelect;
      if (is(select2, SQL)) {
        valuesSqlList.push(select2);
      } else {
        valuesSqlList.push(select2.getSQL());
      }
    } else {
      const values = valuesOrSelect;
      valuesSqlList.push(sql.raw('values '));
      for (const [valueIndex, value] of values.entries()) {
        const valueList: any[] = [];
        for (const [fieldName, col] of colEntries) {
          const colValue = value[fieldName];
          if (colValue === undefined || (is(colValue, Param) && colValue.value === undefined)) {
            if ((col as any).defaultFn !== undefined) {
              const defaultFnResult = (col as any).defaultFn();
              const defaultValue = is(defaultFnResult, SQL)
                ? defaultFnResult
                : sql.param(defaultFnResult, col as any);
              valueList.push(defaultValue);
            } else if (!(col as any).default && (col as any).onUpdateFn !== undefined) {
              const onUpdateFnResult = (col as any).onUpdateFn();
              const newValue = is(onUpdateFnResult, SQL)
                ? onUpdateFnResult
                : sql.param(onUpdateFnResult, col as any);
              valueList.push(newValue);
            } else {
              valueList.push(sql`default`);
            }
          } else {
            valueList.push(colValue);
          }
        }
        valuesSqlList.push(valueList);
        if (valueIndex < values.length - 1) {
          valuesSqlList.push(sql`, `);
        }
      }
    }

    const withSql = this.buildWithCTE(withList);
    const valuesSql = sql.join(valuesSqlList);
    const overridingSql =
      overridingSystemValue_ === true ? sql`overriding system value ` : undefined;
    const insertOrderSql = sql.join(insertOrder as any[], sql.raw(', '));
    return sql`${withSql}insert into ${table} (${insertOrderSql}) ${overridingSql}${valuesSql}`;
  }

  prepareTyping(encoder: any): any {
    if (is(encoder, SnowflakeJson)) {
      return 'json';
    } else if (is(encoder, SnowflakeDecimal)) {
      return 'decimal';
    } else if (is(encoder, SnowflakeTimestamp)) {
      return 'timestamp';
    } else {
      return 'none';
    }
  }

  sqlToQuery(sql2: SQL, invokeSource?: any): any {
    return sql2.toQuery({
      casing: this.casing,
      escapeName: this.escapeName,
      escapeParam: this.escapeParam,
      escapeString: this.escapeString,
      prepareTyping: this.prepareTyping as any,
      invokeSource,
    });
  }
}
