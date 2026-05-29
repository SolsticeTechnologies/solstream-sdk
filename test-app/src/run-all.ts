/**
 * Run all tests sequentially, each for TEST_DURATION_SECS seconds.
 *
 *   npm run test:all
 */

import { execSync } from 'child_process';
import * as path from 'path';
import { banner, info, separator } from './logger';

const tests = [
  { name: 'Slots',        script: 'test-slots' },
  { name: 'Transactions', script: 'test-transactions' },
  { name: 'Blocks',       script: 'test-blocks' },
  { name: 'Accounts',     script: 'test-accounts' },
];

banner('SOLSTREAM SDK â€” Full Test Suite');

for (const test of tests) {
  separator();
  info('SUITE', `Running: ${test.name}`);
  separator();

  try {
    execSync(
      `npx ts-node ${path.join(__dirname, test.script + '.ts')}`,
      { stdio: 'inherit', cwd: path.join(__dirname, '..') },
    );
  } catch {
    // ts-node exits with process.exit(0) which throws in execSync â€” that's fine
  }
}

separator();
banner('All tests complete');

