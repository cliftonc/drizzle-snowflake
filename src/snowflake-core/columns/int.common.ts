import { entityKind } from 'drizzle-orm/entity';
import { SnowflakeColumnBuilder } from './common.ts';

export class SnowflakeIntColumnBaseBuilder<
  T = any,
  TRuntimeConfig extends object = object,
  TTypeConfig extends object = object,
> extends SnowflakeColumnBuilder<any, TRuntimeConfig, TTypeConfig> {
  static readonly [entityKind]: string = 'SnowflakeIntColumnBaseBuilder';

  generatedAlwaysAsIdentity(sequence?: any): any {
    if (sequence) {
      const { name, ...options } = sequence;
      (this.config as any).generatedIdentity = {
        type: 'always',
        sequenceName: name,
        sequenceOptions: options,
      };
    } else {
      (this.config as any).generatedIdentity = {
        type: 'always',
      };
    }
    (this.config as any).hasDefault = true;
    (this.config as any).notNull = true;
    return this;
  }

  generatedByDefaultAsIdentity(sequence?: any): any {
    if (sequence) {
      const { name, ...options } = sequence;
      (this.config as any).generatedIdentity = {
        type: 'byDefault',
        sequenceName: name,
        sequenceOptions: options,
      };
    } else {
      (this.config as any).generatedIdentity = {
        type: 'byDefault',
      };
    }
    (this.config as any).hasDefault = true;
    (this.config as any).notNull = true;
    return this;
  }
}
