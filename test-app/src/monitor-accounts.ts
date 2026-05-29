/**
 * Probe: Account updates
 *
 *   npm run monitor:accounts
 *
 * Runs a probe every PROBE_INTERVAL_SECS (default 5 min):
 *   connect â†’ wait for first account update â†’ disconnect â†’ log â†’ sleep.
 * Filters: SPL token accounts (165 B) and whale wallets (>10 000 SOL).
 */

import { CommitmentLevel } from '@solstream-test/solstream-sdk';
import {
  config,
  alertConfig,
  dynamoConfig,
  PROBE_INTERVAL_MS,
  PROBE_TIMEOUT_MS,
  ALERT_AFTER_FAILURES,
} from './config';
import { banner, info, separator } from './logger';
import { probeSubscribe, runProbeLoop } from './probe';

const TOKEN_PROGRAM  = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA';
const SYSTEM_PROGRAM = '11111111111111111111111111111111';
// 10 000 SOL keeps the startup snapshot small vs >1 SOL which matches millions of accounts
const WHALE_LAMPORTS = BigInt(10_000 * 1_000_000_000);

async function main() {
  banner('PROBE: Account Updates');
  info('CONFIG', `endpoint=${config.endpoint}`);
  info('CONFIG', `interval=${PROBE_INTERVAL_MS / 1000}s  timeout=${PROBE_TIMEOUT_MS / 1000}s  alert_after=${ALERT_AFTER_FAILURES} failures`);
  info('CONFIG', 'filters=spl-token-accounts(165B) funded-wallets(>10k SOL)  commitment=CONFIRMED');
  if (alertConfig) info('ALERT', `to=${alertConfig.to.join(', ')}`);
  if (dynamoConfig) info('DYNAMO', `table=${dynamoConfig.tableName}`);
  separator();

  await runProbeLoop({
    monitorId: 'monitor-accounts',
    endpoint: config.endpoint,
    probeIntervalMs: PROBE_INTERVAL_MS,
    alertAfterFailures: ALERT_AFTER_FAILURES,
    dynamoConfig,
    alertConfig,
    probe: () =>
      probeSubscribe(
        config,
        {
          accounts: {
            'spl-token-accounts': {
              account: [],
              owner: [TOKEN_PROGRAM],
              filters: [{ datasize: BigInt(165) }, { tokenAccountState: true }],
            },
            'funded-wallets': {
              account: [],
              owner: [SYSTEM_PROGRAM],
              filters: [{ lamports: { gt: WHALE_LAMPORTS } }],
            },
          },
          commitment: CommitmentLevel.CONFIRMED,
        },
        PROBE_TIMEOUT_MS,
      ),
  });
}

main().catch((err) => {
  console.error('[FATAL]', err.message);
  process.exit(1);
});

