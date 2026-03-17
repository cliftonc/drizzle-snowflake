import 'dotenv/config';
import { setupAll, teardownAll } from './setup.ts';

const hasCredentials = !!(
  process.env.SNOWFLAKE_ACCOUNT &&
  process.env.SNOWFLAKE_USER &&
  process.env.SNOWFLAKE_PASSWORD
);

export async function setup() {
  if (hasCredentials) {
    await setupAll();
  }
}

export async function teardown() {
  if (hasCredentials) {
    await teardownAll();
  }
}
