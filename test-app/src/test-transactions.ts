/**
 * Test: Transaction updates (non-vote)
 *
 *   npm run test:transactions
 *
 * Subscribes to non-vote transactions and prints a summary of each.
 * Stops after TEST_DURATION_SECS seconds (default 30).
 */

import { subscribe, CommitmentLevel, SubscribeUpdate } from '@solstice/solstream-sdk';
import { config, TEST_DURATION_MS } from './config';
import { banner, info, success, warn, error, stat, separator } from './logger';

async function main() {
  banner('TEST: Transaction Updates');
  info('CONFIG', `endpoint=${config.endpoint}`);
  info('CONFIG', `duration=${TEST_DURATION_MS / 1000}s  commitment=PROCESSED  vote=false  failed=false`);
  separator();

  let count = 0;
  let successCount = 0;
  let failCount = 0;
  const programCounts: Map<string, number> = new Map();

  const stream = await subscribe(
    config,
    {
      transactions: {
        'non-vote': {
          vote: false,
          failed: false,
          accountInclude: [],
          accountExclude: [],
          accountRequired: [],
        },
      },
      commitment: CommitmentLevel.PROCESSED,
    },
    async (update: SubscribeUpdate) => {
      if (!update.transaction) return;
      count++;

      const { transaction, slot } = update.transaction;
      const sig = transaction?.signature
        ? Buffer.from(transaction.signature).toString('hex').slice(0, 16)
        : '???';

      const fee = transaction?.meta?.fee ?? BigInt(0);
      const hasErr = !!transaction?.meta?.err;
      const logCount = transaction?.meta?.logMessages?.length ?? 0;
      const computeUnits = transaction?.meta?.computeUnitsConsumed;

      if (hasErr) {
        failCount++;
        warn('TX', `slot=${slot}  sig=${sig}…  fee=${fee}  logs=${logCount}  [FAILED]`);
      } else {
        successCount++;
        success(
          'TX',
          `slot=${slot}  sig=${sig}…  fee=${fee}  logs=${logCount}` +
          (computeUnits !== undefined ? `  cu=${computeUnits}` : ''),
        );
      }

      // Track program invocations from log messages
      for (const log of transaction?.meta?.logMessages ?? []) {
        const match = log.match(/^Program (\S+) invoke/);
        if (match?.[1]) {
          const prog = match[1].slice(0, 12) + '…';
          programCounts.set(prog, (programCounts.get(prog) ?? 0) + 1);
        }
      }
    },
    (err: Error) => {
      error('STREAM', err.message);
    },
  );

  info('STREAM', `started  id=${stream.id}`);

  const finish = () => {
    stream.cancel();
    separator();
    stat('Total transactions', count);
    stat('Successful', successCount);
    stat('Failed', failCount);

    if (programCounts.size > 0) {
      const top = [...programCounts.entries()]
        .sort((a, b) => b[1] - a[1])
        .slice(0, 5);
      info('TOP PROGRAMS', '');
      for (const [prog, c] of top) {
        stat('  ' + prog, c, 'invocations');
      }
    }

    info('DONE', 'Test complete.');
    process.exit(0);
  };

  setTimeout(finish, TEST_DURATION_MS);
  process.on('SIGINT', finish);
}

main().catch((err) => {
  error('FATAL', err.message);
  process.exit(1);
});
