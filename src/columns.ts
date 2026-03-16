import { customType } from 'drizzle-orm/pg-core';

/**
 * Snowflake VARIANT column type.
 * Stores semi-structured data. Maps to `unknown` in TypeScript.
 */
export const snowflakeVariant = <TData = unknown>(name: string) =>
  customType<{ data: TData; driverData: string | TData }>({
    dataType() {
      return 'VARIANT';
    },
    toDriver(value: TData): string {
      if (typeof value === 'string') {
        return value;
      }
      return JSON.stringify(value);
    },
    fromDriver(value: string | TData): TData {
      if (typeof value === 'string') {
        try {
          return JSON.parse(value) as TData;
        } catch {
          return value as unknown as TData;
        }
      }
      return value as TData;
    },
  })(name);

/**
 * Snowflake ARRAY column type.
 * Stores semi-structured arrays. Maps to `T[]` in TypeScript.
 */
export const snowflakeArray = <TData = unknown>(name: string) =>
  customType<{ data: TData[]; driverData: TData[] | string }>({
    dataType() {
      return 'ARRAY';
    },
    toDriver(value: TData[]): string {
      return JSON.stringify(value);
    },
    fromDriver(value: TData[] | string): TData[] {
      if (typeof value === 'string') {
        try {
          return JSON.parse(value) as TData[];
        } catch {
          return [] as TData[];
        }
      }
      return value;
    },
  })(name);

/**
 * Snowflake OBJECT column type.
 * Stores semi-structured objects. Maps to `Record<string, unknown>` in TypeScript.
 */
export const snowflakeObject = <TData extends Record<string, any> = Record<string, unknown>>(name: string) =>
  customType<{ data: TData; driverData: TData | string }>({
    dataType() {
      return 'OBJECT';
    },
    toDriver(value: TData): string {
      if (typeof value === 'string') {
        return value;
      }
      return JSON.stringify(value);
    },
    fromDriver(value: TData | string): TData {
      if (typeof value === 'string') {
        try {
          return JSON.parse(value) as TData;
        } catch {
          return value as unknown as TData;
        }
      }
      return value;
    },
  })(name);

/**
 * Snowflake TIMESTAMP_LTZ column type (local timezone).
 */
export const snowflakeTimestampLtz = (name: string) =>
  customType<{
    data: Date | string;
    driverData: string | Date;
  }>({
    dataType() {
      return 'TIMESTAMP_LTZ';
    },
    toDriver(value: Date | string): string {
      if (value instanceof Date) {
        return value.toISOString();
      }
      return value;
    },
    fromDriver(value: string | Date): Date {
      if (value instanceof Date) {
        return value;
      }
      const str = String(value);
      const hasOffset =
        str.endsWith('Z') || /[+-]\d{2}:?\d{2}$/.test(str);
      const normalized = hasOffset
        ? str.replace(' ', 'T')
        : `${str.replace(' ', 'T')}Z`;
      return new Date(normalized);
    },
  })(name);

/**
 * Snowflake TIMESTAMP_NTZ column type (no timezone).
 */
export const snowflakeTimestampNtz = (name: string) =>
  customType<{
    data: Date | string;
    driverData: string | Date;
  }>({
    dataType() {
      return 'TIMESTAMP_NTZ';
    },
    toDriver(value: Date | string): string {
      if (value instanceof Date) {
        return value.toISOString();
      }
      return value;
    },
    fromDriver(value: string | Date): Date {
      if (value instanceof Date) {
        return value;
      }
      const str = String(value);
      const normalized = `${str.replace(' ', 'T')}Z`;
      return new Date(normalized);
    },
  })(name);

/**
 * Snowflake TIMESTAMP_TZ column type (with timezone).
 */
export const snowflakeTimestampTz = (name: string) =>
  customType<{
    data: Date | string;
    driverData: string | Date;
  }>({
    dataType() {
      return 'TIMESTAMP_TZ';
    },
    toDriver(value: Date | string): string {
      if (value instanceof Date) {
        return value.toISOString();
      }
      return value;
    },
    fromDriver(value: string | Date): Date {
      if (value instanceof Date) {
        return value;
      }
      const str = String(value);
      const hasOffset =
        str.endsWith('Z') || /[+-]\d{2}:?\d{2}$/.test(str);
      const normalized = hasOffset
        ? str.replace(' ', 'T')
        : `${str.replace(' ', 'T')}Z`;
      return new Date(normalized);
    },
  })(name);

/**
 * Snowflake DATE column type.
 */
export const snowflakeDate = (name: string) =>
  customType<{ data: string | Date; driverData: string | Date }>({
    dataType() {
      return 'DATE';
    },
    toDriver(value: string | Date): string {
      if (value instanceof Date) {
        return value.toISOString().slice(0, 10);
      }
      return value;
    },
    fromDriver(value: string | Date): string {
      if (value instanceof Date) {
        return value.toISOString().slice(0, 10);
      }
      return value.slice(0, 10);
    },
  })(name);

/**
 * Snowflake GEOGRAPHY column type.
 * Stores GeoJSON data.
 */
export const snowflakeGeography = <TData = unknown>(name: string) =>
  customType<{ data: TData; driverData: string | TData }>({
    dataType() {
      return 'GEOGRAPHY';
    },
    toDriver(value: TData): string {
      if (typeof value === 'string') {
        return value;
      }
      return JSON.stringify(value);
    },
    fromDriver(value: string | TData): TData {
      if (typeof value === 'string') {
        try {
          return JSON.parse(value) as TData;
        } catch {
          return value as unknown as TData;
        }
      }
      return value as TData;
    },
  })(name);

/**
 * Snowflake NUMBER(precision, scale) column type.
 * Maps to string for arbitrary precision.
 */
export const snowflakeNumber = (name: string, precision = 38, scale = 0) =>
  customType<{ data: string; driverData: string | number }>({
    dataType() {
      return `NUMBER(${precision}, ${scale})`;
    },
    toDriver(value: string): string {
      return value;
    },
    fromDriver(value: string | number): string {
      return String(value);
    },
  })(name);
