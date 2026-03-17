import { entityKind } from 'drizzle-orm/entity';
import { QueryPromise } from 'drizzle-orm/query-promise';

export class SnowflakeRaw extends QueryPromise<any> {
  static readonly [entityKind]: string = 'SnowflakeRaw';

  constructor(
    public override execute: () => Promise<any>,
    public sql: any,
    public query: any,
    public mapBatchResult: (result: any) => any,
  ) {
    super();
  }

  /** @internal */
  getSQL(): any {
    return this.sql;
  }

  getQuery(): any {
    return this.query;
  }

  mapResult(result: any, isFromBatch?: boolean): any {
    return isFromBatch ? this.mapBatchResult(result) : result;
  }

  _prepare(): this {
    return this;
  }

  /** @internal */
  isResponseInArrayMode(): boolean {
    return false;
  }
}
