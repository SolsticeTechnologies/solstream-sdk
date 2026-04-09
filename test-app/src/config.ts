import * as dotenv from 'dotenv';
import * as path from 'path';
import type { SolstreamConfig } from '@solstice/solstream-sdk';

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
  maxReconnectAttempts: 3,
  baseReconnectDelayMs: 1_000,
  replay: false,
};

export const TEST_DURATION_MS =
  parseInt(process.env['TEST_DURATION_SECS'] ?? '30', 10) * 1_000;

export const alertConfig = process.env['ALERT_FROM'] && process.env['ALERT_TO']
  ? {
      region: process.env['AWS_REGION'] ?? 'us-east-1',
      from: process.env['ALERT_FROM']!,
      to: process.env['ALERT_TO']!.split(',').map(s => s.trim()),
    }
  : null;

export const ALERT_SILENCE_SECS =
  parseInt(process.env['ALERT_SILENCE_SECS'] ?? '60', 10);
