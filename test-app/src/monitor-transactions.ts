/**
 * Probe: Transaction updates
 *
 *   npm run monitor:transactions
 *
 * Runs a probe every PROBE_INTERVAL_SECS (default 5 min):
 *   connect â†’ wait for first transaction update â†’ disconnect â†’ log â†’ sleep.
 * Filters: non-vote token-program txns and system-program txns.
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

async function main() {
  banner('PROBE: Transaction Updates');
  info('CONFIG', `endpoint=${config.endpoint}`);
  info('CONFIG', `interval=${PROBE_INTERVAL_MS / 1000}s  timeout=${PROBE_TIMEOUT_MS / 1000}s  alert_after=${ALERT_AFTER_FAILURES} failures`);
  info('CONFIG', 'filters=token-txns,system-txns  vote=false  failed=false  commitment=PROCESSED');
  if (alertConfig) info('ALERT', `to=${alertConfig.to.join(', ')}`);
  if (dynamoConfig) info('DYNAMO', `table=${dynamoConfig.tableName}`);
  separator();

  await runProbeLoop({
    monitorId: 'monitor-transactions',
    endpoint: config.endpoint,
    probeIntervalMs: PROBE_INTERVAL_MS,
    alertAfterFailures: ALERT_AFTER_FAILURES,
    dynamoConfig,
    alertConfig,
    probe: () =>
      probeSubscribe(
        config,
        {
          transactions: {
            'token-txns': {
              vote: false,
              failed: false,
              accountInclude: [TOKEN_PROGRAM],
              accountExclude: [],
              accountRequired: [],
            },
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
        PROBE_TIMEOUT_MS,
      ),
  });
}

main().catch((err) => {
  console.error('[FATAL]', err.message);
  process.exit(1);
});

