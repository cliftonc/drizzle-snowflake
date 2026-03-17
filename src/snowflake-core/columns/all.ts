import { bigint } from './bigint.ts';
import { boolean } from './boolean.ts';
import { customType } from './custom.ts';
import { date } from './date.ts';
import { decimal } from './decimal.ts';
import { doublePrecision } from './double-precision.ts';
import { integer } from './integer.ts';
import { json } from './json.ts';
import { real } from './real.ts';
import { smallint } from './smallint.ts';
import { text } from './text.ts';
import { timestamp } from './timestamp.ts';
import { varchar } from './varchar.ts';

export type SnowflakeColumnsBuilders = ReturnType<typeof getSnowflakeColumnBuilders>;

export function getSnowflakeColumnBuilders() {
  return {
    bigint,
    boolean,
    customType,
    date,
    decimal,
    doublePrecision,
    integer,
    json,
    real,
    smallint,
    text,
    timestamp,
    varchar,
  };
}
