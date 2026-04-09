/**
 * Monitor: Slot updates (runs indefinitely)
 *
 *   npm run monitor:slots
 *
 * Subscribes to all slot status changes. Streams forever until SIGINT.
 * Sends an email alert via AWS SES if the stream goes silent.
 */

import { subscribe, CommitmentLevel, SubscribeUpdate } from '@solstice/solstream-sdk';
import { config, alertConfig, ALERT_SILENCE_SECS } from './config';
import { banner, info, success, warn, error, stat, separator } from './logger';
import { createHeartbeat } from './alerter';

async function main() {
  banner('MONITOR: Slot Updates');
  info('CONFIG', `endpoint=${config.endpoint}`);
  info('CONFIG', 'running indefinitely  filterByCommitment=false');
  if (alertConfig) info('ALERT', `silence threshold=${ALERT_SILENCE_SECS}s  to=${alertConfig.to.join(', ')}`);
  separator();

  let count = 0;
  const statusCounts: Record<string, number> = {};
  let minSlot = BigInt(Number.MAX_SAFE_INTEGER);
  let maxSlot = BigInt(0);
  const startMs = Date.now();

  const heartbeat = alertConfig
    ? createHeartbeat(alertConfig, 'monitor-slots', ALERT_SILENCE_SECS)
    : null;

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
      heartbeat?.();

      const { slotInfo } = update.slot;
      const slot = slotInfo?.slot ?? BigInt(0);
      const status = String(slotInfo?.status ?? 'UNKNOWN');
      const parent = slotInfo?.parent;

      statusCounts[status] = (statusCounts[status] ?? 0) + 1;
      if (slot < minSlot) minSlot = slot;
      if (slot > maxSlot) maxSlot = slot;

      const log = status.includes('FINALIZED') ? warn : success;
      log(
        'SLOT',
        `slot=${slot}  status=${status}` +
        (parent !== undefined ? `  parent=${parent}` : '') +
        (slotInfo?.deadError ? `  deadError=${slotInfo.deadError}` : ''),
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
    stat('Total slot updates', count);
    stat('Updates/sec', (count / elapsedSecs).toFixed(1));
    if (count > 0) stat('Slot range', `${minSlot} → ${maxSlot}`);
    info('STATUS BREAKDOWN', '');
    for (const [status, c] of Object.entries(statusCounts).sort((a, b) => b[1] - a[1])) {
      stat('  ' + status, c);
    }
    info('DONE', 'Monitor stopped.');
    process.exit(0);
  });
}

main().catch((err) => {
  error('FATAL', err.message);
  process.exit(1);
});
