/**
 * One-shot probe helpers for periodic health checks.
 *
 * Each probe: connects, waits for the first message (or timeout), then
 * disconnects.  The outer loop in each monitor calls these on a schedule.
 */

import { SolstreamClient, ClientError } from '@solstream-test/solstream-sdk';
import type {
  SolstreamConfig,
  SubscribeRequest,
  SubscribeBlockRequest,
} from '@solstream-test/solstream-sdk';

import type { AlertConfig } from './alerter';
import { sendAlert } from './alerter';
import { writeStatus, MonitorStatus } from './status-store';
import { info, success, error } from './logger';

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// Single-shot probe
// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

export interface ProbeResult {
  ok: boolean;
  firstSlot?: bigint;
  errorMessage?: string;
  durationMs: number;
}

/** Connect, wait for the first decoded update, then disconnect. */
export async function probeSubscribe(
  config: SolstreamConfig,
  request: SubscribeRequest,
  timeoutMs: number,
): Promise<ProbeResult> {
  const startMs = Date.now();
  const client = SolstreamClient.connect(config.endpoint, config.apiKey ?? '', config.channelOptions);
  const stream = client.messages().withRawRequest(request).subscribe();
  const timer = setTimeout(() => stream.cancel(), timeoutMs);
  try {
    const update = await stream.next();
    clearTimeout(timer);
    client.close();
    if (!update) return { ok: false, errorMessage: 'stream ended with no data', durationMs: Date.now() - startMs };
    return { ok: true, firstSlot: update.slot(), durationMs: Date.now() - startMs };
  } catch (err) {
    clearTimeout(timer);
    client.close();
    if (err instanceof ClientError && err.kind === 'cancelled') {
      return { ok: false, errorMessage: `no data within ${timeoutMs / 1000}s`, durationMs: Date.now() - startMs };
    }
    return { ok: false, errorMessage: err instanceof Error ? err.message : String(err), durationMs: Date.now() - startMs };
  }
}

/** Connect, wait for the first decoded block, then disconnect. */
export async function probeBlocks(
  config: SolstreamConfig,
  request: SubscribeBlockRequest,
  timeoutMs: number,
): Promise<ProbeResult> {
  const startMs = Date.now();
  const client = SolstreamClient.connect(config.endpoint, config.apiKey ?? '', config.channelOptions);
  const stream = client.blocks().withRawRequest(request).subscribe();
  const timer = setTimeout(() => stream.cancel(), timeoutMs);
  try {
    const update = await stream.next();
    clearTimeout(timer);
    client.close();
    if (!update) return { ok: false, errorMessage: 'stream ended with no data', durationMs: Date.now() - startMs };
    return { ok: true, firstSlot: update.block.slot, durationMs: Date.now() - startMs };
  } catch (err) {
    clearTimeout(timer);
    client.close();
    if (err instanceof ClientError && err.kind === 'cancelled') {
      return { ok: false, errorMessage: `no data within ${timeoutMs / 1000}s`, durationMs: Date.now() - startMs };
    }
    return { ok: false, errorMessage: err instanceof Error ? err.message : String(err), durationMs: Date.now() - startMs };
  }
}

// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// Probe loop
// â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

export interface ProbeLoopConfig {
  monitorId: string;
  endpoint: string;
  probeIntervalMs: number;
  /** Number of consecutive failures before sending an alert email. */
  alertAfterFailures: number;
  dynamoConfig: { tableName: string; region: string } | null;
  alertConfig: AlertConfig | null;
  /** The probe to run on each cycle. */
  probe: () => Promise<ProbeResult>;
}

/**
 * Run a probe loop forever: probe â†’ write status â†’ sleep â†’ repeat.
 * Never returns (process must be killed or receive SIGINT).
 */
export async function runProbeLoop(cfg: ProbeLoopConfig): Promise<never> {
  let probeCount = 0;
  let successCount = 0;
  let lastSuccessAt: string | null = null;
  let consecutiveFailures = 0;
  let alertSent = false;

  const writeStatusRecord = async (status: MonitorStatus) => {
    if (!cfg.dynamoConfig) return;
    await writeStatus(cfg.dynamoConfig.tableName, cfg.dynamoConfig.region, {
      monitorId: cfg.monitorId,
      status,
      endpoint: cfg.endpoint,
      lastDataAt: lastSuccessAt,
      updatedAt: new Date().toISOString(),
      totalUpdates: successCount,
      updatesPerSec: '0.00',
      consecutiveFailures,
    });
  };

  // Mark STARTING so the admin panel is never stale after a restart.
  await writeStatusRecord('STARTING').catch(() => {});

  process.on('SIGINT', () => {
    info('PROBE', 'received SIGINT â€” shutting down');
    process.exit(0);
  });

  // eslint-disable-next-line no-constant-condition
  while (true) {
    probeCount++;
    info('PROBE', `#${probeCount} startingâ€¦`);

    const result = await cfg.probe();

    if (result.ok) {
      successCount++;
      lastSuccessAt = new Date().toISOString();
      consecutiveFailures = 0;
      success('PROBE', `#${probeCount} ok  slot=${result.firstSlot ?? '?'}  ${result.durationMs}ms`);

      writeStatusRecord('ONLINE').catch(console.error);

      if (alertSent && cfg.alertConfig) {
        alertSent = false;
        const subject = `[Solstream] RECOVERED: ${cfg.monitorId} is responding`;
        const body = [
          `Monitor: ${cfg.monitorId}`,
          `Status: RECOVERED`,
          `Time: ${new Date().toISOString()}`,
          '',
          'The probe is receiving data again.',
        ].join('\n');
        sendAlert(cfg.alertConfig, subject, body).catch(console.error);
      }
    } else {
      consecutiveFailures++;
      error('PROBE', `#${probeCount} failed (${consecutiveFailures} in a row) â€” ${result.errorMessage}`);

      // Always write SILENT immediately so updatedAt stays fresh and the admin
      // panel reflects the real state.
      writeStatusRecord('SILENT').catch(console.error);

      // Send alert on first failure — don't wait for threshold so a prolonged
      // outage is never missed. alertSent prevents duplicate emails until recovery.
      if (!alertSent && cfg.alertConfig) {
        const subject = `[Solstream] ALERT: ${cfg.monitorId} stream not responding`;
        const body = [
          `Monitor: ${cfg.monitorId}`,
          `Status: SILENT`,
          `Time: ${new Date().toISOString()}`,
          '',
          `Probe failed after ${consecutiveFailures} attempt(s).`,
          `Last error: ${result.errorMessage}`,
          `Endpoint: ${cfg.endpoint}`,
          '',
          'Check the EC2 instance and Solstream endpoint.',
        ].join('\n');
        sendAlert(cfg.alertConfig, subject, body)
          .then(() => { alertSent = true; })
          .catch((err) => { console.error('[ALERT] Failed to send alert email:', err); });
      }
    }

    info('PROBE', `next in ${cfg.probeIntervalMs / 1000}s`);
    await sleep(cfg.probeIntervalMs);
  }
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

