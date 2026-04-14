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
  const startMs = Date.now();

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

      const slot = update.block?.slot ?? BigInt(0);
      const blockhash = update.block?.blockhash?.slice(0, 12) ?? '???';
      const txCount = update.transactions.length;
      const blockTime = update.block?.blockTime;
      const blockHeight = update.block?.blockHeight;

      totalTxns += txCount;

      let blockFees = BigInt(0);
      for (const tx of update.transactions) {
        blockFees += tx.meta?.fee ?? BigInt(0);
      }
      totalFees += blockFees;

      success(
        'BLOCK',
        `slot=${slot}  hash=${blockhash}…  ` +
        `txns=${txCount}  fees=${lamportsToSol(blockFees)}` +
        (blockHeight !== undefined ? `  height=${blockHeight}` : '') +
        (blockTime !== undefined ? `  time=${new Date(Number(blockTime) * 1000).toISOString().slice(11, 19)}` : ''),
      );
    },
    (err: Error) => {
      statusUpdater?.markError();
      error('STREAM', err.message);
    },
  );

  info('STREAM', `started  id=${stream.id}`);

  process.on('SIGINT', async () => {
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
