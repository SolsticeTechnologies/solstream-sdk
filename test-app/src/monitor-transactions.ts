/**
 * Monitor: Transaction updates (runs indefinitely)
 *
 *   npm run monitor:transactions
 *
 * Subscribes to filtered non-vote transactions. Streams forever until SIGINT.
 * Filters:
 *   - token-txns:  transactions involving the SPL Token program
 *   - system-txns: transactions involving the System program
 * Sends an email alert via AWS SES if the stream goes silent.
 * Writes live status to DynamoDB for the admin panel.
 */

import { subscribe, CommitmentLevel, SubscribeUpdate } from '@solstice/solstream-sdk';
import { config, alertConfig, ALERT_SILENCE_SECS, dynamoConfig } from './config';
import { banner, info, success, error, stat, separator } from './logger';
import { createHeartbeat } from './alerter';
import { createStatusUpdater } from './status-store';

async function main() {
  banner('MONITOR: Transaction Updates');
  info('CONFIG', `endpoint=${config.endpoint}`);
  info('CONFIG', 'running indefinitely  commitment=PROCESSED  vote=false  failed=false  filters=token-txns,system-txns');
  if (alertConfig) info('ALERT', `silence threshold=${ALERT_SILENCE_SECS}s  to=${alertConfig.to.join(', ')}`);
  if (dynamoConfig) info('DYNAMO', `table=${dynamoConfig.tableName}  flush=${dynamoConfig.flushIntervalMs / 1000}s`);
  separator();

  let count = 0;
  let successCount = 0;
  let failCount = 0;
  let windowCount = 0;
  let windowFailed = 0;
  let lastSlot: bigint = BigInt(0);
  let lastSig = '???';
  const programCounts: Map<string, number> = new Map();
  const startMs = Date.now();

  const logInterval = setInterval(() => {
    if (windowCount === 0) {
      info('TX', 'no updates in last 30s — waiting for data…');
    } else {
      const msg = windowFailed > 0
        ? `${windowCount} txns/30s (${windowFailed} failed)  last slot=${lastSlot}  last sig=${lastSig}…`
        : `${windowCount} txns/30s  last slot=${lastSlot}  last sig=${lastSig}…`;
      success('TX', msg);
      windowCount = 0;
      windowFailed = 0;
    }
  }, 30_000);
  logInterval.unref();

  const statusUpdater = dynamoConfig
    ? createStatusUpdater(dynamoConfig.tableName, dynamoConfig.region, 'monitor-transactions', config.endpoint, dynamoConfig.flushIntervalMs)
    : null;

  const heartbeat = alertConfig
    ? createHeartbeat(alertConfig, 'monitor-transactions', ALERT_SILENCE_SECS, statusUpdater ?? undefined)
    : null;

  const TOKEN_PROGRAM  = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA';
  const SYSTEM_PROGRAM = '11111111111111111111111111111111';

  const stream = await subscribe(
    config,
    {
      transactions: {
        // Non-vote transactions involving the SPL Token program
        'token-txns': {
          vote: false,
          failed: false,
          accountInclude: [TOKEN_PROGRAM],
          accountExclude: [],
          accountRequired: [],
        },
        // Non-vote transactions involving the System program
        'system-txns': {
          vote: false,
          failed: false,
          accountInclude: [SYSTEM_PROGRAM],
          accountExclude: [],
          accountRequired: [],
        },
      },
      commitment: CommitmentLevel.PROCESSED,
    },
    async (update: SubscribeUpdate) => {
      if (!update.transaction) return;
      count++;
      heartbeat?.();
      statusUpdater?.tick();

      const { transaction, slot } = update.transaction;
      const hasErr = !!transaction?.transactionMeta?.err;

      windowCount++;
      lastSlot = slot;
      lastSig = transaction?.signature
        ? Buffer.from(transaction.signature).toString('hex').slice(0, 16)
        : '???';

      if (hasErr) {
        failCount++;
        windowFailed++;
      } else {
        successCount++;
      }

      for (const log of transaction?.transactionMeta?.logMessages ?? []) {
        const match = log.match(/^Program (\S+) invoke/);
        if (match?.[1]) {
          const prog = match[1].slice(0, 12) + '…';
          programCounts.set(prog, (programCounts.get(prog) ?? 0) + 1);
        }
      }
    },
    (err: Error) => {
      error('STREAM', `reconnecting — ${err.message}`);
    },
  );

  info('STREAM', `started  id=${stream.id}`);

  process.on('SIGINT', async () => {
    clearInterval(logInterval);
    stream.cancel();
    await statusUpdater?.stop();
    const elapsedSecs = (Date.now() - startMs) / 1000;
    separator();
    stat('Total transactions', count);
    stat('Successful', successCount);
    stat('Failed', failCount);
    stat('Rate', (count / elapsedSecs).toFixed(1), 'tx/sec');
    if (programCounts.size > 0) {
      const top = [...programCounts.entries()].sort((a, b) => b[1] - a[1]).slice(0, 5);
      info('TOP PROGRAMS', '');
      for (const [prog, c] of top) stat('  ' + prog, c, 'invocations');
    }
    info('DONE', 'Monitor stopped.');
    process.exit(0);
  });
}

main().catch((err) => {
  error('FATAL', err.message);
  process.exit(1);
});
