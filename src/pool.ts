import snowflake from 'snowflake-sdk';
import {
  closeClientConnection,
  promisifyConnect,
  type SnowflakeConnection,
  type SnowflakeConnectionPool,
} from './client.ts';

export interface SnowflakePoolConfig {
  /** Maximum concurrent connections. Defaults to 4. */
  size?: number;
}

export interface SnowflakeConnectionPoolOptions {
  /** Maximum concurrent connections. Defaults to 4. */
  size?: number;
  /** Timeout in milliseconds to wait for a connection. Defaults to 30000 (30s). */
  acquireTimeout?: number;
  /** Maximum number of requests waiting for a connection. Defaults to 100. */
  maxWaitingRequests?: number;
  /** Max time (ms) a connection may live before being recycled. */
  maxLifetimeMs?: number;
  /** Max idle time (ms) before an idle connection is discarded. */
  idleTimeoutMs?: number;
}

export type SnowflakeConnectionConfig = snowflake.ConnectionOptions;

export function createSnowflakeConnectionPool(
  connectionOptions: SnowflakeConnectionConfig,
  options: SnowflakeConnectionPoolOptions = {}
): SnowflakeConnectionPool & { size: number } {
  const size = options.size && options.size > 0 ? options.size : 4;
  const acquireTimeout = options.acquireTimeout ?? 30_000;
  const maxWaitingRequests = options.maxWaitingRequests ?? 100;
  const maxLifetimeMs = options.maxLifetimeMs;
  const idleTimeoutMs = options.idleTimeoutMs;
  const metadata = new WeakMap<
    SnowflakeConnection,
    { createdAt: number; lastUsedAt: number }
  >();

  type PooledConnection = {
    connection: SnowflakeConnection;
    createdAt: number;
    lastUsedAt: number;
  };

  const idle: PooledConnection[] = [];
  const waiting: Array<{
    resolve: (conn: SnowflakeConnection) => void;
    reject: (error: Error) => void;
    timeoutId: ReturnType<typeof setTimeout>;
  }> = [];
  let total = 0;
  let closed = false;
  let pendingAcquires = 0;

  const shouldRecycle = (conn: PooledConnection, now: number): boolean => {
    if (maxLifetimeMs !== undefined && now - conn.createdAt >= maxLifetimeMs) {
      return true;
    }
    if (idleTimeoutMs !== undefined && now - conn.lastUsedAt >= idleTimeoutMs) {
      return true;
    }
    return false;
  };

  const createConnection = async (): Promise<SnowflakeConnection> => {
    const conn = snowflake.createConnection(connectionOptions);
    return promisifyConnect(conn);
  };

  const acquire = async (): Promise<SnowflakeConnection> => {
    if (closed) {
      throw new Error('Snowflake connection pool is closed');
    }

    while (idle.length > 0) {
      const pooled = idle.pop() as PooledConnection;
      const now = Date.now();
      if (shouldRecycle(pooled, now)) {
        await closeClientConnection(pooled.connection);
        total = Math.max(0, total - 1);
        metadata.delete(pooled.connection);
        continue;
      }
      pooled.lastUsedAt = now;
      metadata.set(pooled.connection, {
        createdAt: pooled.createdAt,
        lastUsedAt: pooled.lastUsedAt,
      });
      return pooled.connection;
    }

    if (total < size) {
      pendingAcquires += 1;
      total += 1;
      try {
        const connection = await createConnection();
        if (closed) {
          await closeClientConnection(connection);
          total -= 1;
          throw new Error('Snowflake connection pool is closed');
        }
        const now = Date.now();
        metadata.set(connection, { createdAt: now, lastUsedAt: now });
        return connection;
      } catch (error) {
        total -= 1;
        throw error;
      } finally {
        pendingAcquires -= 1;
      }
    }

    if (waiting.length >= maxWaitingRequests) {
      throw new Error(
        `Snowflake connection pool queue is full (max ${maxWaitingRequests} waiting requests)`
      );
    }

    return await new Promise((resolve, reject) => {
      const timeoutId = setTimeout(() => {
        const idx = waiting.findIndex((w) => w.timeoutId === timeoutId);
        if (idx !== -1) {
          waiting.splice(idx, 1);
        }
        reject(
          new Error(
            `Snowflake connection pool acquire timeout after ${acquireTimeout}ms`
          )
        );
      }, acquireTimeout);

      waiting.push({ resolve, reject, timeoutId });
    });
  };

  const release = async (connection: SnowflakeConnection): Promise<void> => {
    const waiter = waiting.shift();
    if (waiter) {
      clearTimeout(waiter.timeoutId);
      const now = Date.now();
      const meta =
        metadata.get(connection) ??
        ({ createdAt: now, lastUsedAt: now } as {
          createdAt: number;
          lastUsedAt: number;
        });

      const expired =
        maxLifetimeMs !== undefined && now - meta.createdAt >= maxLifetimeMs;

      if (closed) {
        await closeClientConnection(connection);
        total = Math.max(0, total - 1);
        metadata.delete(connection);
        waiter.reject(new Error('Snowflake connection pool is closed'));
        return;
      }

      if (expired) {
        await closeClientConnection(connection);
        total = Math.max(0, total - 1);
        metadata.delete(connection);
        try {
          const replacement = await acquire();
          waiter.resolve(replacement);
        } catch (error) {
          waiter.reject(error as Error);
        }
        return;
      }

      meta.lastUsedAt = now;
      metadata.set(connection, meta);
      waiter.resolve(connection);
      return;
    }

    if (closed) {
      await closeClientConnection(connection);
      metadata.delete(connection);
      total = Math.max(0, total - 1);
      return;
    }

    const now = Date.now();
    const existingMeta =
      metadata.get(connection) ??
      ({ createdAt: now, lastUsedAt: now } as {
        createdAt: number;
        lastUsedAt: number;
      });
    existingMeta.lastUsedAt = now;
    metadata.set(connection, existingMeta);

    if (
      maxLifetimeMs !== undefined &&
      now - existingMeta.createdAt >= maxLifetimeMs
    ) {
      await closeClientConnection(connection);
      total -= 1;
      metadata.delete(connection);
      return;
    }

    idle.push({
      connection,
      createdAt: existingMeta.createdAt,
      lastUsedAt: existingMeta.lastUsedAt,
    });
  };

  const close = async (): Promise<void> => {
    closed = true;

    const waiters = waiting.splice(0, waiting.length);
    for (const waiter of waiters) {
      clearTimeout(waiter.timeoutId);
      waiter.reject(new Error('Snowflake connection pool is closed'));
    }

    const toClose = idle.splice(0, idle.length);
    await Promise.allSettled(
      toClose.map((item) => closeClientConnection(item.connection))
    );
    total = Math.max(0, total - toClose.length);
    toClose.forEach((item) => metadata.delete(item.connection));

    const maxWait = 5000;
    const start = Date.now();
    while (pendingAcquires > 0 && Date.now() - start < maxWait) {
      await new Promise((r) => setTimeout(r, 10));
    }
  };

  return {
    acquire,
    release,
    close,
    size,
  };
}
