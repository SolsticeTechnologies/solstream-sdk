/**
 * Test: Slot updates
 *
 *   npm run test:slots
 *
 * Subscribes to all slot status changes and tracks processing throughput.
 * Stops after TEST_DURATION_SECS seconds (default 30).
 */

import { subscribe, CommitmentLevel, SubscribeUpdate } from '@solstice/solstream-sdk';
import { config, TEST_DURATION_MS } from './config';
import { banner, info, success, warn, error, stat, separator } from './logger';

async function main() {
  banner('TEST: Slot Updates');
  info('CONFIG', `endpoint=${config.endpoint}`);
  info('CONFIG', `duration=${TEST_DURATION_MS / 1000}s  filterByCommitment=false`);
  separator();

  let count = 0;
  const statusCounts: Record<string, number> = {};
  let minSlot = BigInt(Number.MAX_SAFE_INTEGER);
  let maxSlot = BigInt(0);
  const startMs = Date.now();

  const stream = await subscribe(
    config,
    {
      slots: {
        all: { filterByCommitment: false },
      },
      commitment: CommitmentLevel.PROCESSED,
    },
    async (update: SubscribeUpdate) => {
      if (!update.slot) return;
      count++;

      const { slotInfo } = update.slot;
      const slot = slotInfo?.slot ?? BigInt(0);
      const status = String(slotInfo?.status ?? 'UNKNOWN');
      const parent = slotInfo?.parent;

      statusCounts[status] = (statusCounts[status] ?? 0) + 1;
      if (slot < minSlot) minSlot = slot;
      if (slot > maxSlot) maxSlot = slot;

      const log = status.includes('Dead') ? warn : success;
      log(
        'SLOT',
        `slot=${slot}  status=${status}` +
        (parent !== undefined ? `  parent=${parent}` : '') +
        '',
      );
    },
    (err: Error) => {
      error('STREAM', err.message);
    },
  );

  info('STREAM', `started  id=${stream.id}`);

  const finish = () => {
    stream.cancel();
    const elapsedSecs = (Date.now() - startMs) / 1000;
    separator();
    stat('Total slot updates', count);
    stat('Updates/sec', (count / elapsedSecs).toFixed(1));
    if (count > 0) {
      stat('Slot range', `${minSlot} → ${maxSlot}`);
    }
    info('STATUS BREAKDOWN', '');
    for (const [status, c] of Object.entries(statusCounts).sort((a, b) => b[1] - a[1])) {
      stat('  ' + status, c);
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
