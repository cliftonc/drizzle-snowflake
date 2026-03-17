import 'dotenv/config';
import { setupAll, teardownAll } from './setup.ts';

export async function setup() {
  await setupAll();
}

export async function teardown() {
  await teardownAll();
}
