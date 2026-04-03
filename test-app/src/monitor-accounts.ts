/**
 * Monitor: Account updates (runs indefinitely)
 *
 *   npm run monitor:accounts
 *
 * Subscribes to all account updates. Streams forever until SIGINT.
 */

import { subscribe, CommitmentLevel, SubscribeUpdate } from '@solstice/solstream-sdk';
import { config } from './config';
import { banner, info, success, error, stat, separator, lamportsToSol } from './logger';

async function main() {
  banner('MONITOR: Account Updates');
  info('CONFIG', `endpoint=${config.endpoint}`);
  info('CONFIG', 'running indefinitely  commitment=CONFIRMED');
  separator();

  let count = 0;
  let startupCount = 0;
  const startMs = Date.now();

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

  process.on('SIGINT', () => {
    stream.cancel();
    const elapsedSecs = (Date.now() - startMs) / 1000;
    separator();
    stat('Total account updates', count);
    stat('Startup updates', startupCount);
    stat('Rate', (count / elapsedSecs).toFixed(1), 'updates/sec');
    info('DONE', 'Monitor stopped.');
    process.exit(0);
  });
}

main().catch((err) => {
  error('FATAL', err.message);
  process.exit(1);
});
