/**
 * Monitor: Block updates (runs indefinitely)
 *
 *   npm run monitor:blocks
 *
 * Subscribes to full block updates with transactions. Streams forever until SIGINT.
 * Sends an email alert via AWS SES if the stream goes silent.
 * Writes live status to DynamoDB for the admin panel.
 */

import { subscribeBlocks, SubscribeBlockUpdate } from '@solstice/solstream-sdk';
import { config, alertConfig, ALERT_SILENCE_SECS, dynamoConfig } from './config';
import { banner, info, success, error, stat, separator, lamportsToSol } from './logger';
import { createHeartbeat } from './alerter';
import { createStatusUpdater } from './status-store';

async function main() {
  banner('MONITOR: Block Updates');
  info('CONFIG', `endpoint=${config.endpoint}`);
  info('CONFIG', 'running indefinitely  includeTransactions=true');
  if (alertConfig) info('ALERT', `silence threshold=${ALERT_SILENCE_SECS}s  to=${alertConfig.to.join(', ')}`);
  if (dynamoConfig) info('DYNAMO', `table=${dynamoConfig.tableName}  flush=${dynamoConfig.flushIntervalMs / 1000}s`);
  separator();

  let blockCount = 0;
  let totalTxns = 0;
  let totalFees = BigInt(0);
  let windowBlocks = 0;
  let windowTxns = 0;
  let lastSlot: bigint = BigInt(0);
  const startMs = Date.now();

  const logInterval = setInterval(() => {
    if (windowBlocks === 0) {
      info('BLOCK', 'no updates in last 30s — waiting for data…');
    } else {
      success('BLOCK', `${windowBlocks} blocks/30s  ${windowTxns} txns  last slot=${lastSlot}`);
      windowBlocks = 0;
      windowTxns = 0;
    }
  }, 30_000);
  logInterval.unref();

  const statusUpdater = dynamoConfig
    ? createStatusUpdater(dynamoConfig.tableName, dynamoConfig.region, 'monitor-blocks', config.endpoint, dynamoConfig.flushIntervalMs)
    : null;

  const heartbeat = alertConfig
    ? createHeartbeat(alertConfig, 'monitor-blocks', ALERT_SILENCE_SECS, statusUpdater ?? undefined)
    : null;

  const stream = await subscribeBlocks(
    config,
    {
      includeTransactions: true,
      includeAccounts: false,
      includeEntries: false,
    },
    async (update: SubscribeBlockUpdate) => {
      blockCount++;
      heartbeat?.();
      statusUpdater?.tick();

      const txCount = update.transactions.length;
      lastSlot = update.block?.slot ?? BigInt(0);
      windowBlocks++;
      windowTxns += txCount;
      totalTxns += txCount;

      for (const tx of update.transactions) {
        totalFees += tx.transactionMeta?.fee ?? BigInt(0);
      }
    },
    (err: Error) => {
      statusUpdater?.markError();
      error('STREAM', err.message);
    },
  );

  info('STREAM', `started  id=${stream.id}`);

  process.on('SIGINT', async () => {
    clearInterval(logInterval);
    stream.cancel();
    await statusUpdater?.stop();
    const elapsedSecs = (Date.now() - startMs) / 1000;
    separator();
    stat('Blocks received', blockCount);
    stat('Total transactions', totalTxns);
    stat('Total fees', lamportsToSol(totalFees));
    if (blockCount > 0) {
      stat('Avg txns/block', (totalTxns / blockCount).toFixed(1));
      stat('Blocks/sec', (blockCount / elapsedSecs).toFixed(3));
    }
    info('DONE', 'Monitor stopped.');
    process.exit(0);
  });
}

main().catch((err) => {
  error('FATAL', err.message);
  process.exit(1);
});
