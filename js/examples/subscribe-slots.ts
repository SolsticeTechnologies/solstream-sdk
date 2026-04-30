/**
 * Example: Stream confirmed slot updates with automatic reconnection.
 *
 * Mirrors the Rust SDK reconnect example. If the connection drops, the stream
 * transparently reconnects from the last seen slot so no slots are skipped.
 *
 * Set the following environment variables before running:
 *   SOLSTREAM_ENDPOINT  - gRPC endpoint, e.g. https://stream.example.com
 *   SOLSTREAM_API_KEY   - Your API key
 *
 * Run:
 *   npx ts-node examples/subscribe-slots.ts
 */

import { SolstreamClient, CommitmentLevel, SlotStatus } from '../src';
import type { ReconnectConfig } from '../src';

async function main() {
  const endpoint = process.env['SOLSTREAM_ENDPOINT'] ?? 'https://stream.example.com';
  const apiKey   = process.env['SOLSTREAM_API_KEY'] ?? '';

  const client = SolstreamClient.connect(endpoint, apiKey);

  // Retry up to 10 times with exponential backoff starting at 500 ms, capped at 30 s.
  // Pass an empty object `{}` for unlimited retries with defaults.
  const reconnect: ReconnectConfig = {
    maxAttempts: 10,
    initialBackoffMs: 500,
    maxBackoffMs: 30_000,
    backoffFactor: 2.0,
  };

  const stream = client
    .messages()
    .slots('slots')
      .filterByCommitment(true)
      .build()
    .commitment(CommitmentLevel.CONFIRMED)
    .reconnect(reconnect)
    .subscribe();

  console.log('Streaming confirmed slots (Ctrl-C to stop)…\n');

  process.on('SIGINT', () => {
    console.log('\nCancelling…');
    stream.cancel();
    process.exit(0);
  });

  while (true) {
    const update = await stream.next();
    if (!update) break;
    const payload = update.decode();
    if (payload.kind === 'slot' && payload.data.status === SlotStatus.Confirmed) {
      console.log(`slot ${payload.data.slot}  (reconnects: ${stream.attempts()})`);
    }
  }

  console.log('Stream closed.');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
