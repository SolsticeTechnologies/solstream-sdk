/**
 * Probe: Slot updates
 *
 *   npm run monitor:slots
 *
 * Runs a probe every PROBE_INTERVAL_SECS (default 5 min):
 *   connect â†’ wait for first slot update â†’ disconnect â†’ log â†’ sleep.
 * Writes status to DynamoDB and sends an alert if consecutive probes fail.
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

async function main() {
  banner('PROBE: Slot Updates');
  info('CONFIG', `endpoint=${config.endpoint}`);
  info('CONFIG', `interval=${PROBE_INTERVAL_MS / 1000}s  timeout=${PROBE_TIMEOUT_MS / 1000}s  alert_after=${ALERT_AFTER_FAILURES} failures`);
  if (alertConfig) info('ALERT', `to=${alertConfig.to.join(', ')}`);
  if (dynamoConfig) info('DYNAMO', `table=${dynamoConfig.tableName}`);
  separator();

  await runProbeLoop({
    monitorId: 'monitor-slots',
    endpoint: config.endpoint,
    probeIntervalMs: PROBE_INTERVAL_MS,
    alertAfterFailures: ALERT_AFTER_FAILURES,
    dynamoConfig,
    alertConfig,
    probe: () =>
      probeSubscribe(
        config,
        {
          slots: { all: { filterByCommitment: false } },
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

