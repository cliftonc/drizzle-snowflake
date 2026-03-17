import { entityKind } from 'drizzle-orm/entity';
// @ts-expect-error - getColumnNameAndConfig exists at runtime
import { getColumnNameAndConfig } from 'drizzle-orm/utils';
import { SnowflakeColumn, SnowflakeColumnBuilder } from './common.ts';

export class SnowflakeCustomColumnBuilder extends SnowflakeColumnBuilder {
  static readonly [entityKind]: string = 'SnowflakeCustomColumnBuilder';

  constructor(name: string, fieldConfig: any, customTypeParams: any) {
    super(name, 'custom', 'SnowflakeCustomColumn');
    (this.config as any).fieldConfig = fieldConfig;
    (this.config as any).customTypeParams = customTypeParams;
  }

  /** @internal */
  build(table: any): SnowflakeCustomColumn {
    return new SnowflakeCustomColumn(table, this.config as any);
  }
}

export class SnowflakeCustomColumn extends SnowflakeColumn {
  static readonly [entityKind]: string = 'SnowflakeCustomColumn';

  sqlName: string;
  mapTo: ((value: any) => any) | undefined;
  mapFrom: ((value: any) => any) | undefined;

  constructor(table: any, config: any) {
    super(table, config);
    this.sqlName = config.customTypeParams.dataType(config.fieldConfig);
    this.mapTo = config.customTypeParams.toDriver;
    this.mapFrom = config.customTypeParams.fromDriver;
  }

  getSQLType(): string {
    return this.sqlName;
  }

  mapFromDriverValue(value: any): any {
    return typeof this.mapFrom === 'function' ? this.mapFrom(value) : value;
  }

  mapToDriverValue(value: any): any {
    return typeof this.mapTo === 'function' ? this.mapTo(value) : value;
  }
}

export function customType<T extends { data: any; driverData?: any; config?: Record<string, any> }>(
  customTypeParams: {
    dataType: (config?: T['config']) => string;
    toDriver?: (value: T['data']) => T extends { driverData: infer D } ? D : T['data'];
    fromDriver?: (value: T extends { driverData: infer D } ? D : T['data']) => T['data'];
  },
): (a?: any, b?: any) => SnowflakeCustomColumnBuilder {
  return (a?: any, b?: any) => {
    const { name, config } = getColumnNameAndConfig(a, b);
    return new SnowflakeCustomColumnBuilder(name, config, customTypeParams);
  };
}
