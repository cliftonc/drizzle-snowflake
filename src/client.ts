import type snowflake from 'snowflake-sdk';

export type SnowflakeConnection = snowflake.Connection;

export interface SnowflakeConnectionPool {
  acquire(): Promise<SnowflakeConnection>;
  release(connection: SnowflakeConnection): void | Promise<void>;
  close?(): Promise<void> | void;
}

export type SnowflakeClientLike = SnowflakeConnection | SnowflakeConnectionPool;
export type RowData = Record<string, unknown>;

export type ExecuteArraysResult = { columns: string[]; rows: unknown[][] };

export function isPool(
  client: SnowflakeClientLike
): client is SnowflakeConnectionPool {
  return typeof (client as SnowflakeConnectionPool).acquire === 'function';
}

export function promisifyConnect(
  connection: SnowflakeConnection
): Promise<SnowflakeConnection> {
  return new Promise((resolve, reject) => {
    connection.connect((err, conn) => {
      if (err) {
        reject(err);
      } else {
        resolve(conn);
      }
    });
  });
}

export function promisifyExecute(
  connection: SnowflakeConnection,
  sqlText: string,
  binds: unknown[]
): Promise<{ statement: snowflake.RowStatement; rows: RowData[] }> {
  return new Promise((resolve, reject) => {
    connection.execute({
      sqlText,
      binds: binds as snowflake.Binds,
      complete(err, statement, rows) {
        if (err) {
          reject(err);
        } else {
          resolve({
            statement: statement as snowflake.RowStatement,
            rows: (rows ?? []) as RowData[],
          });
        }
      },
    });
  });
}

export function prepareParams(params: unknown[]): unknown[] {
  return params.map((param) => {
    if (param === undefined) return null;
    if (param instanceof Date) return param.toISOString();
    if (typeof param === 'bigint') return param.toString();
    return param;
  });
}

function deduplicateColumns(columns: string[]): string[] {
  const counts = new Map<string, number>();
  let hasDuplicates = false;

  for (const column of columns) {
    const next = (counts.get(column) ?? 0) + 1;
    counts.set(column, next);
    if (next > 1) {
      hasDuplicates = true;
      break;
    }
  }

  if (!hasDuplicates) {
    return columns;
  }

  counts.clear();
  return columns.map((column) => {
    const count = counts.get(column) ?? 0;
    counts.set(column, count + 1);
    return count === 0 ? column : `${column}_${count}`;
  });
}

export async function executeOnClient(
  client: SnowflakeClientLike,
  query: string,
  params: unknown[]
): Promise<RowData[]> {
  if (isPool(client)) {
    const connection = await client.acquire();
    try {
      return await executeOnClient(connection, query, params);
    } finally {
      await client.release(connection);
    }
  }

  const prepared = prepareParams(params);
  const { rows } = await promisifyExecute(client, query, prepared);

  if (!rows || rows.length === 0) {
    return [];
  }

  return rows;
}

export async function executeArraysOnClient(
  client: SnowflakeClientLike,
  query: string,
  params: unknown[]
): Promise<ExecuteArraysResult> {
  if (isPool(client)) {
    const connection = await client.acquire();
    try {
      return await executeArraysOnClient(connection, query, params);
    } finally {
      await client.release(connection);
    }
  }

  const prepared = prepareParams(params);
  const { statement, rows } = await promisifyExecute(client, query, prepared);

  const stmtColumns = statement.getColumns();
  const columnNames = deduplicateColumns(
    stmtColumns.map((col: snowflake.Column) => col.getName())
  );

  const arrayRows: unknown[][] = (rows ?? []).map((row) =>
    columnNames.map((_name, idx) => {
      const originalName = stmtColumns[idx]!.getName();
      return row[originalName] ?? null;
    })
  );

  return { columns: columnNames, rows: arrayRows };
}

export function closeClientConnection(
  connection: SnowflakeConnection
): Promise<void> {
  return new Promise((resolve, reject) => {
    connection.destroy((err) => {
      if (err) {
        reject(err);
      } else {
        resolve();
      }
    });
  });
}
