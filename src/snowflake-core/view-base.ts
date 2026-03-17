import { entityKind } from 'drizzle-orm/entity';
import { View } from 'drizzle-orm/sql/sql';

export class SnowflakeViewBase extends View {
  static readonly [entityKind]: string = 'SnowflakeViewBase';
}
