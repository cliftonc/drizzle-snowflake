import { describe, expect, it } from 'vitest';
import {
  snowflakeArray,
  snowflakeDate,
  snowflakeGeography,
  snowflakeNumber,
  snowflakeObject,
  snowflakeTimestampLtz,
  snowflakeTimestampNtz,
  snowflakeTimestampTz,
  snowflakeVariant,
} from '../src/columns.ts';
import { snowflakeTable } from '../src/index.ts';

// Helper: extract the custom type's toDriver/fromDriver via the column config
function getMappers(columnFn: (name: string) => any) {
  const table = snowflakeTable('test', { col: columnFn('col') });
  const col = table.col as any;
  return {
    toDriver: (v: any) => col.mapToDriverValue(v),
    fromDriver: (v: any) => col.mapFromDriverValue(v),
    dataType: col.getSQLType(),
  };
}

describe('snowflakeVariant', () => {
  const { toDriver, fromDriver, dataType } = getMappers(snowflakeVariant);

  it('should have VARIANT data type', () => {
    expect(dataType).toBe('VARIANT');
  });

  it('should serialize objects to JSON strings', () => {
    expect(toDriver({ key: 'value' })).toBe('{"key":"value"}');
  });

  it('should pass through string values', () => {
    expect(toDriver('already a string')).toBe('already a string');
  });

  it('should deserialize JSON strings to objects', () => {
    expect(fromDriver('{"key":"value"}')).toEqual({ key: 'value' });
  });

  it('should pass through non-string values', () => {
    const obj = { key: 'value' };
    expect(fromDriver(obj)).toBe(obj);
  });

  it('should handle non-JSON strings gracefully', () => {
    expect(fromDriver('not json')).toBe('not json');
  });
});

describe('snowflakeArray', () => {
  const { toDriver, fromDriver, dataType } = getMappers(snowflakeArray);

  it('should have ARRAY data type', () => {
    expect(dataType).toBe('ARRAY');
  });

  it('should serialize arrays to JSON', () => {
    expect(toDriver([1, 2, 3])).toBe('[1,2,3]');
  });

  it('should deserialize JSON strings to arrays', () => {
    expect(fromDriver('[1,2,3]')).toEqual([1, 2, 3]);
  });

  it('should pass through array values', () => {
    const arr = [1, 2, 3];
    expect(fromDriver(arr)).toBe(arr);
  });

  it('should return empty array for invalid JSON', () => {
    expect(fromDriver('not json')).toEqual([]);
  });
});

describe('snowflakeObject', () => {
  const { toDriver, fromDriver, dataType } = getMappers(snowflakeObject);

  it('should have OBJECT data type', () => {
    expect(dataType).toBe('OBJECT');
  });

  it('should serialize objects to JSON', () => {
    expect(toDriver({ a: 1 })).toBe('{"a":1}');
  });

  it('should pass through string values in toDriver', () => {
    expect(toDriver('{"a":1}')).toBe('{"a":1}');
  });

  it('should deserialize JSON strings to objects', () => {
    expect(fromDriver('{"a":1}')).toEqual({ a: 1 });
  });

  it('should pass through object values', () => {
    const obj = { a: 1 };
    expect(fromDriver(obj)).toBe(obj);
  });
});

describe('snowflakeTimestampLtz', () => {
  const { toDriver, fromDriver, dataType } = getMappers(snowflakeTimestampLtz);

  it('should have TIMESTAMP_LTZ data type', () => {
    expect(dataType).toBe('TIMESTAMP_LTZ');
  });

  it('should convert Date to ISO string', () => {
    const date = new Date('2024-01-15T12:00:00Z');
    expect(toDriver(date)).toBe('2024-01-15T12:00:00.000Z');
  });

  it('should pass through string values in toDriver', () => {
    expect(toDriver('2024-01-15T12:00:00Z')).toBe('2024-01-15T12:00:00Z');
  });

  it('should parse ISO string to Date', () => {
    const result = fromDriver('2024-01-15T12:00:00Z');
    expect(result).toBeInstanceOf(Date);
    expect((result as Date).toISOString()).toBe('2024-01-15T12:00:00.000Z');
  });

  it('should parse space-separated timestamp to Date', () => {
    const result = fromDriver('2024-01-15 12:00:00+00:00');
    expect(result).toBeInstanceOf(Date);
    expect((result as Date).toISOString()).toBe('2024-01-15T12:00:00.000Z');
  });

  it('should pass through Date values', () => {
    const date = new Date('2024-01-15T12:00:00Z');
    expect(fromDriver(date)).toBe(date);
  });
});

describe('snowflakeTimestampNtz', () => {
  const { toDriver, fromDriver, dataType } = getMappers(snowflakeTimestampNtz);

  it('should have TIMESTAMP_NTZ data type', () => {
    expect(dataType).toBe('TIMESTAMP_NTZ');
  });

  it('should parse timestamp without timezone as UTC', () => {
    const result = fromDriver('2024-01-15 12:00:00');
    expect(result).toBeInstanceOf(Date);
    expect((result as Date).toISOString()).toBe('2024-01-15T12:00:00.000Z');
  });
});

describe('snowflakeTimestampTz', () => {
  const { toDriver, fromDriver, dataType } = getMappers(snowflakeTimestampTz);

  it('should have TIMESTAMP_TZ data type', () => {
    expect(dataType).toBe('TIMESTAMP_TZ');
  });

  it('should parse timestamp with offset', () => {
    const result = fromDriver('2024-01-15 12:00:00+05:30');
    expect(result).toBeInstanceOf(Date);
    expect((result as Date).toISOString()).toBe('2024-01-15T06:30:00.000Z');
  });
});

describe('snowflakeDate', () => {
  const { toDriver, fromDriver, dataType } = getMappers(snowflakeDate);

  it('should have DATE data type', () => {
    expect(dataType).toBe('DATE');
  });

  it('should convert Date to YYYY-MM-DD string', () => {
    expect(toDriver(new Date('2024-01-15T12:00:00Z'))).toBe('2024-01-15');
  });

  it('should pass through date strings', () => {
    expect(toDriver('2024-01-15')).toBe('2024-01-15');
  });

  it('should extract date portion from Date', () => {
    const result = fromDriver(new Date('2024-01-15T12:00:00Z'));
    expect(result).toBe('2024-01-15');
  });

  it('should extract date portion from string', () => {
    expect(fromDriver('2024-01-15T12:00:00Z')).toBe('2024-01-15');
  });
});

describe('snowflakeGeography', () => {
  const { toDriver, fromDriver, dataType } = getMappers(snowflakeGeography);

  it('should have GEOGRAPHY data type', () => {
    expect(dataType).toBe('GEOGRAPHY');
  });

  it('should serialize GeoJSON to string', () => {
    const point = { type: 'Point', coordinates: [0, 0] };
    expect(toDriver(point)).toBe('{"type":"Point","coordinates":[0,0]}');
  });

  it('should deserialize GeoJSON string', () => {
    expect(fromDriver('{"type":"Point","coordinates":[0,0]}')).toEqual({
      type: 'Point',
      coordinates: [0, 0],
    });
  });
});

describe('snowflakeNumber', () => {
  it('should have NUMBER(p,s) data type', () => {
    const { dataType } = getMappers((n) => snowflakeNumber(n, 10, 2));
    expect(dataType).toBe('NUMBER(10, 2)');
  });

  it('should default to NUMBER(38, 0)', () => {
    const { dataType } = getMappers(snowflakeNumber);
    expect(dataType).toBe('NUMBER(38, 0)');
  });

  it('should convert numbers to strings', () => {
    const { fromDriver } = getMappers(snowflakeNumber);
    expect(fromDriver(42)).toBe('42');
    expect(fromDriver(3.14)).toBe('3.14');
  });

  it('should pass through string values', () => {
    const { fromDriver } = getMappers(snowflakeNumber);
    expect(fromDriver('12345')).toBe('12345');
  });
});
