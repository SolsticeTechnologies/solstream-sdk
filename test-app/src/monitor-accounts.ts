/**
 * Monitor: Account updates (runs indefinitely)
 *
 *   npm run monitor:accounts
 *
 * Subscribes to filtered account updates. Streams forever until SIGINT.
 * Filters:
 *   - spl-token-accounts: Token program accounts, exactly 165 bytes
 *   - funded-wallets:     System program accounts with > 1 SOL
 * Sends an email alert via AWS SES if the stream goes silent.
 * Writes live status to DynamoDB for the admin panel.
 */

import { subscribe, CommitmentLevel, SubscribeUpdate } from '@solstice/solstream-sdk';
import { config, alertConfig, ALERT_SILENCE_SECS, dynamoConfig } from './config';
import { banner, info, success, error, stat, separator } from './logger';
import { createHeartbeat } from './alerter';
import { createStatusUpdater } from './status-store';

async function main() {
  banner('MONITOR: Account Updates');
  info('CONFIG', `endpoint=${config.endpoint}`);
  info('CONFIG', 'running indefinitely  commitment=CONFIRMED  filters=spl-token-accounts,funded-wallets');
  if (alertConfig) info('ALERT', `silence threshold=${ALERT_SILENCE_SECS}s  to=${alertConfig.to.join(', ')}`);
  if (dynamoConfig) info('DYNAMO', `table=${dynamoConfig.tableName}  flush=${dynamoConfig.flushIntervalMs / 1000}s`);
  separator();

  let count = 0;
  let startupCount = 0;
  let windowCount = 0;
  let lastSlot: bigint = BigInt(0);
  let lastPubkey = '???';
  const startMs = Date.now();

  const LOG_INTERVAL_MS = 30_000;
  const logInterval = setInterval(() => {
    if (windowCount === 0) {
      info('ACCOUNT', 'no updates in last 30s — waiting for data…');
    } else {
      success(
        'ACCOUNT',
        `${windowCount} updates/30s  last slot=${lastSlot}  last pubkey=${lastPubkey}…`,
      );
      windowCount = 0;
    }
  }, LOG_INTERVAL_MS);
  logInterval.unref();

  const statusUpdater = dynamoConfig
    ? createStatusUpdater(dynamoConfig.tableName, dynamoConfig.region, 'monitor-accounts', config.endpoint, dynamoConfig.flushIntervalMs)
    : null;

  const heartbeat = alertConfig
    ? createHeartbeat(alertConfig, 'monitor-accounts', ALERT_SILENCE_SECS, statusUpdater ?? undefined)
    : null;

  const TOKEN_PROGRAM  = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA';
  const SYSTEM_PROGRAM = '11111111111111111111111111111111';
  const ONE_SOL        = BigInt(1_000_000_000);

  const stream = await subscribe(
    config,
    {
      accounts: {
        // SPL token accounts: owned by Token program, exactly 165 bytes
        'spl-token-accounts': {
          account: [],
          owner: [TOKEN_PROGRAM],
          filters: [
            { datasize: BigInt(165) },
            { tokenAccountState: true },
          ],
        },
        // System-owned accounts with more than 1 SOL
        'funded-wallets': {
          account: [],
          owner: [SYSTEM_PROGRAM],
          filters: [
            { lamports: { gt: ONE_SOL } },
          ],
        },
      },
      commitment: CommitmentLevel.CONFIRMED,
    },
    async (update: SubscribeUpdate) => {
      if (!update.account) return;
      count++;
      heartbeat?.();
      statusUpdater?.tick();

      const { account, slot, isStartup } = update.account;
      if (isStartup) startupCount++;
      windowCount++;
      lastSlot = slot;
      lastPubkey = account?.pubkey
        ? Buffer.from(account.pubkey).toString('hex').slice(0, 16)
        : '???';
    },
    (err: Error) => {
      statusUpdater?.markReconnecting();
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
