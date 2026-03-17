import { entityKind, is } from 'drizzle-orm/entity';
import { SQL } from 'drizzle-orm/sql/sql';
import { IndexedColumn } from './columns/common.ts';

export type AnyIndexBuilder = IndexBuilder;

export class IndexBuilderOn {
  static readonly [entityKind]: string = 'SnowflakeIndexBuilderOn';

  constructor(
    private unique: boolean,
    private name: string,
  ) {}

  on(...columns: any[]): IndexBuilder {
    return new IndexBuilder(
      columns.map((it: any) => {
        if (is(it, SQL)) {
          return it;
        }
        const clonedIndexedColumn = new IndexedColumn(it.name, !!it.keyAsName, it.columnType, it.indexConfig);
        it.indexConfig = JSON.parse(JSON.stringify(it.defaultConfig));
        return clonedIndexedColumn;
      }),
      this.unique,
      this.name,
    );
  }
}

export class IndexBuilder {
  static readonly [entityKind]: string = 'SnowflakeIndexBuilder';

  /** @internal */
  config: {
    name: string;
    columns: any[];
    unique: boolean;
  };

  constructor(columns: any[], unique: boolean, name: string) {
    this.config = {
      name,
      columns,
      unique,
    };
  }

  /** @internal */
  build(table: any): Index {
    return new Index(this.config, table);
  }
}

export class Index {
  static readonly [entityKind]: string = 'SnowflakeIndex';

  config: any;

  constructor(config: any, table: any) {
    this.config = { ...config, table };
  }
}

export function index(name: string): IndexBuilderOn {
  return new IndexBuilderOn(false, name);
}

export function uniqueIndex(name: string): IndexBuilderOn {
  return new IndexBuilderOn(true, name);
}
