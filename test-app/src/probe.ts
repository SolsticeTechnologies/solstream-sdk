/**
 * One-shot probe helpers for periodic health checks.
 *
 * Each probe: connects, waits for the first message (or timeout), then
 * disconnects.  The outer loop in each monitor calls these on a schedule.
 */

import { subscribe, subscribeBlocks } from '@solstice/solstream-sdk';
import type {
  SolstreamConfig,
  SubscribeRequest,
  SubscribeBlockRequest,
  StreamHandle,
} from '@solstice/solstream-sdk';

import type { AlertConfig } from './alerter';
import { sendAlert } from './alerter';
import { writeStatus, MonitorStatus } from './status-store';
import { info, success, error } from './logger';

// ─────────────────────────────────────────────────────────────────────────────
// Single-shot probe
// ─────────────────────────────────────────────────────────────────────────────

export interface ProbeResult {
  ok: boolean;
  firstSlot?: bigint;
  errorMessage?: string;
  durationMs: number;
}

/** Connect, wait for the first SubscribeUpdate, then cancel. */
export async function probeSubscribe(
  config: SolstreamConfig,
  request: SubscribeRequest,
  timeoutMs: number,
): Promise<ProbeResult> {
  const startMs = Date.now();
  // No reconnects during a probe — fail fast on connection error.
  const probeConfig: SolstreamConfig = { ...config, maxReconnectAttempts: 0 };
  let handle: StreamHandle | null = null;
  let resolved = false;

  return new Promise<ProbeResult>((resolve) => {
    const done = (result: ProbeResult) => {
      if (resolved) return;
      resolved = true;
      clearTimeout(timer);
      handle?.cancel();
      resolve(result);
    };

    const timer = setTimeout(() => {
      done({
        ok: false,
        errorMessage: `no data within ${timeoutMs / 1000}s`,
        durationMs: Date.now() - startMs,
      });
    }, timeoutMs);

    subscribe(
      probeConfig,
      request,
      (update) => {
        const slot =
          update.slot?.slotInfo?.slot ??
          update.transaction?.slot ??
          update.account?.slot ??
          update.blockMeta?.blockInfo?.slot;
        done({ ok: true, firstSlot: slot, durationMs: Date.now() - startMs });
      },
      (err) => {
        done({ ok: false, errorMessage: err.message, durationMs: Date.now() - startMs });
      },
    )
      .then((h) => {
        handle = h;
        if (resolved) h.cancel(); // timeout fired before subscribe returned
      })
      .catch((err) => {
        done({ ok: false, errorMessage: String(err), durationMs: Date.now() - startMs });
      });
  });
}

/** Connect, wait for the first SubscribeBlockUpdate, then cancel. */
export async function probeBlocks(
  config: SolstreamConfig,
  request: SubscribeBlockRequest,
  timeoutMs: number,
): Promise<ProbeResult> {
  const startMs = Date.now();
  const probeConfig: SolstreamConfig = { ...config, maxReconnectAttempts: 0 };
  let handle: StreamHandle | null = null;
  let resolved = false;

  return new Promise<ProbeResult>((resolve) => {
    const done = (result: ProbeResult) => {
      if (resolved) return;
      resolved = true;
      clearTimeout(timer);
      handle?.cancel();
      resolve(result);
    };

    const timer = setTimeout(() => {
      done({
        ok: false,
        errorMessage: `no data within ${timeoutMs / 1000}s`,
        durationMs: Date.now() - startMs,
      });
    }, timeoutMs);

    subscribeBlocks(
      probeConfig,
      request,
      (update) => {
        done({ ok: true, firstSlot: update.block?.slot, durationMs: Date.now() - startMs });
      },
      (err) => {
        done({ ok: false, errorMessage: err.message, durationMs: Date.now() - startMs });
      },
    )
      .then((h) => {
        handle = h;
        if (resolved) h.cancel();
      })
      .catch((err) => {
        done({ ok: false, errorMessage: String(err), durationMs: Date.now() - startMs });
      });
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// Probe loop
// ─────────────────────────────────────────────────────────────────────────────

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
 * Run a probe loop forever: probe → write status → sleep → repeat.
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
    });
  };

  // Mark STARTING so the admin panel is never stale after a restart.
  await writeStatusRecord('STARTING').catch(() => {});

  process.on('SIGINT', () => {
    info('PROBE', 'received SIGINT — shutting down');
    process.exit(0);
  });

  // eslint-disable-next-line no-constant-condition
  while (true) {
    probeCount++;
    info('PROBE', `#${probeCount} starting…`);

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
      error('PROBE', `#${probeCount} failed (${consecutiveFailures} in a row) — ${result.errorMessage}`);

      writeStatusRecord('SILENT').catch(console.error);

      if (!alertSent && consecutiveFailures >= cfg.alertAfterFailures && cfg.alertConfig) {
        alertSent = true;
        const subject = `[Solstream] ALERT: ${cfg.monitorId} stream not responding`;
        const body = [
          `Monitor: ${cfg.monitorId}`,
          `Status: NO DATA`,
          `Time: ${new Date().toISOString()}`,
          '',
          `${consecutiveFailures} consecutive probes failed.`,
          `Last error: ${result.errorMessage}`,
          '',
          'Check the EC2 instance and Solstream endpoint.',
        ].join('\n');
        sendAlert(cfg.alertConfig, subject, body).catch(console.error);
      }
    }

    info('PROBE', `next in ${cfg.probeIntervalMs / 1000}s`);
    await sleep(cfg.probeIntervalMs);
  }
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
