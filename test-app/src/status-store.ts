/**
 * DynamoDB status store.
 * Writes monitor health/stats so an admin panel can display live status.
 *
 * Table schema (single-table):
 *   PK  monitorId  (String)  e.g. "monitor-slots"
 *   Attributes: status, lastDataAt, updatedAt, totalUpdates, updatesPerSec, endpoint
 */

import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand } from '@aws-sdk/lib-dynamodb';

export type MonitorStatus = 'STARTING' | 'ONLINE' | 'RECONNECTING' | 'SILENT' | 'ERROR';

export interface StatusRecord {
  monitorId: string;
  status: MonitorStatus;
  endpoint: string;
  lastDataAt: string | null;
  updatedAt: string;
  totalUpdates: number;
  updatesPerSec: string;
}

let client: DynamoDBDocumentClient | null = null;

function getClient(region: string): DynamoDBDocumentClient {
  if (!client) {
    client = DynamoDBDocumentClient.from(new DynamoDBClient({ region }));
  }
  return client;
}

export async function writeStatus(
  tableName: string,
  region: string,
  record: StatusRecord,
): Promise<void> {
  const db = getClient(region);
  await db.send(new PutCommand({ TableName: tableName, Item: record }));
}

/**
 * Returns a status updater bound to a specific monitor.
 * Call updateStatus() on data events and status changes.
 * Also sets up a periodic write every FLUSH_INTERVAL_MS so the table
 * stays fresh even when data is arriving faster than we need to write.
 */
export function createStatusUpdater(
  tableName: string,
  region: string,
  monitorId: string,
  endpoint: string,
  flushIntervalMs = 30_000,
) {
  let totalUpdates = 0;
  let lastDataAt: string | null = null;
  let currentStatus: MonitorStatus = 'STARTING';
  let hasReceivedData = false;
  const startMs = Date.now();

  const flush = async (status: MonitorStatus) => {
    currentStatus = status;
    const elapsedSecs = (Date.now() - startMs) / 1000;
    const record: StatusRecord = {
      monitorId,
      status,
      endpoint,
      lastDataAt,
      updatedAt: new Date().toISOString(),
      totalUpdates,
      updatesPerSec: elapsedSecs > 0 ? (totalUpdates / elapsedSecs).toFixed(2) : '0.00',
    };
    try {
      await writeStatus(tableName, region, record);
    } catch (err) {
      console.error(`[STATUS] Failed to write to DynamoDB:`, err);
    }
  };

  // Periodic flush while ONLINE — only after first data has been received
  const interval = setInterval(() => {
    if (hasReceivedData && currentStatus === 'ONLINE') flush('ONLINE');
  }, flushIntervalMs);
  interval.unref(); // don't block process exit

  // Write STARTING immediately so the admin panel is never stale after a restart
  flush('STARTING').catch(() => {});

  return {
    /** Call on every data update received from the stream */
    tick() {
      totalUpdates++;
      lastDataAt = new Date().toISOString();
      hasReceivedData = true;
      if (currentStatus !== 'ONLINE') flush('ONLINE');
    },
    /** Call when the stream disconnects and is attempting to reconnect */
    markReconnecting() {
      flush('RECONNECTING');
    },
    /** Call when the stream goes silent / alert fires */
    markSilent() {
      flush('SILENT');
    },
    /** Call on stream error */
    markError() {
      flush('ERROR');
    },
    /** Call on recovery */
    markOnline() {
      flush('ONLINE');
    },
    /** Call on graceful shutdown */
    async stop() {
      clearInterval(interval);
      await flush(currentStatus === 'ONLINE' ? 'ONLINE' : currentStatus);
    },
  };
}
