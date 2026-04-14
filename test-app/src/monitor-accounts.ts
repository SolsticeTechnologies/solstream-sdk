/**
 * Monitor: Account updates (runs indefinitely)
 *
 *   npm run monitor:accounts
 *
 * Subscribes to all account updates. Streams forever until SIGINT.
 * Sends an email alert via AWS SES if the stream goes silent.
 * Writes live status to DynamoDB for the admin panel.
 */

import { subscribe, CommitmentLevel, SubscribeUpdate } from '@solstice/solstream-sdk';
import { config, alertConfig, ALERT_SILENCE_SECS, dynamoConfig } from './config';
import { banner, info, success, error, stat, separator, lamportsToSol } from './logger';
import { createHeartbeat } from './alerter';
import { createStatusUpdater } from './status-store';

async function main() {
  banner('MONITOR: Account Updates');
  info('CONFIG', `endpoint=${config.endpoint}`);
  info('CONFIG', 'running indefinitely  commitment=CONFIRMED');
  if (alertConfig) info('ALERT', `silence threshold=${ALERT_SILENCE_SECS}s  to=${alertConfig.to.join(', ')}`);
  if (dynamoConfig) info('DYNAMO', `table=${dynamoConfig.tableName}  flush=${dynamoConfig.flushIntervalMs / 1000}s`);
  separator();

  let count = 0;
  let startupCount = 0;
  const startMs = Date.now();

  const statusUpdater = dynamoConfig
    ? createStatusUpdater(dynamoConfig.tableName, dynamoConfig.region, 'monitor-accounts', config.endpoint, dynamoConfig.flushIntervalMs)
    : null;

  const heartbeat = alertConfig
    ? createHeartbeat(alertConfig, 'monitor-accounts', ALERT_SILENCE_SECS, statusUpdater ?? undefined)
    : null;

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
      heartbeat?.();
      statusUpdater?.tick();

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
