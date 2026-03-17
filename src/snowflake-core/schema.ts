import { entityKind, is } from 'drizzle-orm/entity';
import { SQL, sql } from 'drizzle-orm/sql/sql';
import { snowflakeTableWithSchema } from './table.ts';

export class SnowflakeSchema {
  static readonly [entityKind]: string = 'SnowflakeSchema';

  readonly schemaName: string;
  constructor(schemaName: string) {
    this.schemaName = schemaName;
  }

  table = (name: string, columns: any, extraConfig?: any) => {
    return snowflakeTableWithSchema(name, columns, extraConfig, this.schemaName);
  };

  getSQL(): SQL {
    return new SQL([sql.identifier(this.schemaName)]);
  }

  shouldOmitSQLParens(): boolean {
    return true;
  }
}

export function isSnowflakeSchema(obj: unknown): obj is SnowflakeSchema {
  return is(obj, SnowflakeSchema);
}

export function snowflakeSchema(name: string): SnowflakeSchema {
  return new SnowflakeSchema(name);
}
