/**
 * Email alerter via AWS SES.
 * Sends a notification when a monitor stream goes silent.
 */

import { SESClient, SendRawEmailCommand } from '@aws-sdk/client-ses';
import type { createStatusUpdater } from './status-store';

export interface AlertConfig {
  region: string;
  from: string;
  to: string[];
}

function buildRawEmail(config: AlertConfig, subject: string, body: string): Uint8Array {
  const toHeader = config.to.join(', ');
  const raw = [
    `From: ${config.from}`,
    `To: ${toHeader}`,
    `Subject: ${subject}`,
    'MIME-Version: 1.0',
    'Content-Type: text/plain; charset=UTF-8',
    '',
    body,
  ].join('\r\n');
  return Buffer.from(raw);
}

export async function sendAlert(config: AlertConfig, subject: string, body: string): Promise<void> {
  const client = new SESClient({ region: config.region });
  const command = new SendRawEmailCommand({
    Source: config.from,
    Destinations: config.to,
    RawMessage: { Data: buildRawEmail(config, subject, body) },
  });
  await client.send(command);
}

/**
 * Returns a heartbeat function. Call it every time data is received.
 * If no data arrives within `silenceSecs`, an alert email is sent once.
 * Resets automatically when data resumes.
 */
export function createHeartbeat(
  alertConfig: AlertConfig,
  monitorName: string,
  silenceSecs: number,
  statusUpdater?: ReturnType<typeof createStatusUpdater>,
): () => void {
  let timer: ReturnType<typeof setTimeout> | null = null;
  let alertSent = false;

  const reset = () => {
    if (timer) clearTimeout(timer);

    // Data resumed — send recovery email if we had previously alerted
    if (alertSent) {
      alertSent = false;
      statusUpdater?.markOnline();
      const subject = `[Solstream] RECOVERED: ${monitorName} is receiving data again`;
      const body = [
        `Monitor: ${monitorName}`,
        `Status: RECOVERED`,
        `Time: ${new Date().toISOString()}`,
        '',
        'The stream is receiving data again.',
      ].join('\n');
      sendAlert(alertConfig, subject, body).catch(console.error);
    }

    timer = setTimeout(async () => {
      if (alertSent) return;
      alertSent = true;
      statusUpdater?.markSilent();

      const subject = `[Solstream] ALERT: ${monitorName} stream is silent`;
      const body = [
        `Monitor: ${monitorName}`,
        `Status: NO DATA`,
        `Time: ${new Date().toISOString()}`,
        '',
        `No data received for ${silenceSecs} seconds.`,
        'The stream may be down or the endpoint is unreachable.',
        '',
        'Check the EC2 instance and Solstream endpoint.',
      ].join('\n');

      try {
        await sendAlert(alertConfig, subject, body);
        console.error(`[ALERT] Email sent — ${monitorName} stream silent for ${silenceSecs}s`);
      } catch (err) {
        console.error('[ALERT] Failed to send alert email:', err);
      }
    }, silenceSecs * 1000);
  };

  // Timer only starts after first data is received (first call to the returned fn).
  // This prevents SILENT being written to DynamoDB during container startup/reconnect.
  return reset;
}
