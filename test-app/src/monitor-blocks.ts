/**
 * Probe: Block updates
 *
 *   npm run monitor:blocks
 *
 * Runs a probe every PROBE_INTERVAL_SECS (default 5 min):
 *   connect → wait for first block → disconnect → log → sleep.
 */

import {
  config,
  alertConfig,
  dynamoConfig,
  PROBE_INTERVAL_MS,
  PROBE_TIMEOUT_MS,
  ALERT_AFTER_FAILURES,
} from './config';
import { banner, info, separator } from './logger';
import { probeBlocks, runProbeLoop } from './probe';

async function main() {
  banner('PROBE: Block Updates');
  info('CONFIG', `endpoint=${config.endpoint}`);
  info('CONFIG', `interval=${PROBE_INTERVAL_MS / 1000}s  timeout=${PROBE_TIMEOUT_MS / 1000}s  alert_after=${ALERT_AFTER_FAILURES} failures`);
  info('CONFIG', 'includeTransactions=false  includeAccounts=false  includeEntries=false');
  if (alertConfig) info('ALERT', `to=${alertConfig.to.join(', ')}`);
  if (dynamoConfig) info('DYNAMO', `table=${dynamoConfig.tableName}`);
  separator();

  await runProbeLoop({
    monitorId: 'monitor-blocks',
    endpoint: config.endpoint,
    probeIntervalMs: PROBE_INTERVAL_MS,
    alertAfterFailures: ALERT_AFTER_FAILURES,
    dynamoConfig,
    alertConfig,
    probe: () =>
      probeBlocks(
        config,
        {
          includeTransactions: false,
          includeAccounts: false,
          includeEntries: false,
        },
        PROBE_TIMEOUT_MS,
      ),
  });
}

main().catch((err) => {
  console.error('[FATAL]', err.message);
  process.exit(1);
});
