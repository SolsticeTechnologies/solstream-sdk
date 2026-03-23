/**
 * Test: Account updates
 *
 *   npm run test:accounts
 *
 * Subscribes to ALL account updates and prints a summary of each.
 * Stops after TEST_DURATION_SECS seconds (default 30).
 */

import { subscribe, CommitmentLevel, SubscribeUpdate } from '@solstice/solstream-sdk';
import { config, TEST_DURATION_MS } from './config';
import { banner, info, success, error, stat, separator, lamportsToSol } from './logger';

async function main() {
  banner('TEST: Account Updates');
  info('CONFIG', `endpoint=${config.endpoint}`);
  info('CONFIG', `duration=${TEST_DURATION_MS / 1000}s  commitment=CONFIRMED`);
  separator();

  let count = 0;
  let startupCount = 0;

  const stream = await subscribe(
    config,
    {
      accounts: {
        all: { account: [], owner: [], filters: [] },
      },
      commitment: CommitmentLevel.CONFIRMED,
    },
    async (update: SubscribeUpdate) => {
      if (!update.account) return;
      count++;

      const { account, slot, isStartup } = update.account;
      if (isStartup) startupCount++;

      const pubkey = account?.pubkey
        ? Buffer.from(account.pubkey).toString('hex').slice(0, 16)
        : '???';

      success(
        'ACCOUNT',
        `slot=${slot}  pubkey=${pubkey}…  ` +
        `balance=${lamportsToSol(account?.lamports)}  ` +
        `dataLen=${account?.data?.length ?? 0}B` +
        (isStartup ? '  [startup]' : ''),
      );
    },
    (err: Error) => {
      error('STREAM', err.message);
    },
  );

  info('STREAM', `started  id=${stream.id}`);

  // Auto-stop after duration
  setTimeout(() => {
    stream.cancel();
    separator();
    stat('Total account updates', count);
    stat('Startup updates', startupCount);
    info('DONE', 'Stream cancelled. Test complete.');
    process.exit(0);
  }, TEST_DURATION_MS);

  process.on('SIGINT', () => {
    stream.cancel();
    separator();
    stat('Total account updates', count);
    info('DONE', 'Interrupted. Test complete.');
    process.exit(0);
  });
}

main().catch((err) => {
  error('FATAL', err.message);
  process.exit(1);
});
