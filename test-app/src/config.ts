import * as dotenv from 'dotenv';
import * as path from 'path';
import type { SolstreamConfig } from '@solstream-test/solstream-sdk';

// Load .env from test-app root
dotenv.config({ path: path.resolve(__dirname, '../.env') });

function require_env(name: string): string {
  const val = process.env[name];
  if (!val) {
    console.error(`\n  ERROR: ${name} is not set.\n`);
    console.error(`  Copy test-app/.env.example to test-app/.env and fill in your values.\n`);
    process.exit(1);
  }
  return val;
}

export const config: SolstreamConfig = {
  endpoint: require_env('SOLSTREAM_ENDPOINT'),
  apiKey: process.env['SOLSTREAM_API_KEY'],
  baseReconnectDelayMs: 1_000,
  maxReconnectDelayMs: 30_000,
  replay: false,
};

export const TEST_DURATION_MS =
  parseInt(process.env['TEST_DURATION_SECS'] ?? '30', 10) * 1_000;

/** How long each probe waits for the first message before timing out (default: 30 s). */
export const PROBE_TIMEOUT_MS =
  parseInt(process.env['PROBE_TIMEOUT_SECS'] ?? '30', 10) * 1_000;

/** How long to sleep between probe cycles (default: 5 min). */
export const PROBE_INTERVAL_MS =
  parseInt(process.env['PROBE_INTERVAL_SECS'] ?? '300', 10) * 1_000;

/** Number of consecutive probe failures before sending an alert email (default: 2). */
export const ALERT_AFTER_FAILURES =
  parseInt(process.env['ALERT_AFTER_FAILURES'] ?? '2', 10);

const DEFAULT_REGION = 'eu-west-2';

export const alertConfig = process.env['ALERT_FROM'] && process.env['ALERT_TO']
  ? {
      region: process.env['AWS_REGION_SES'] ?? process.env['AWS_REGION'] ?? DEFAULT_REGION,
      from: process.env['ALERT_FROM']!,
      to: process.env['ALERT_TO']!.split(',').map(s => s.trim()),
    }
  : null;

export const dynamoConfig = process.env['DYNAMODB_TABLE']
  ? {
      tableName: process.env['DYNAMODB_TABLE']!,
      region: process.env['AWS_REGION_DYNAMO'] ?? process.env['AWS_REGION'] ?? DEFAULT_REGION,
    }
  : null;

