/**
 * Example: Subscribe to slot status updates
 *
 * Set the following environment variables before running:
 *   SOLSTREAM_ENDPOINT  - gRPC endpoint, e.g. https://stream.example.com
 *   SOLSTREAM_API_KEY   - Your API key
 *
 * Run:
 *   npx ts-node examples/subscribe-slots.ts
 */

import {
  subscribe,
  CommitmentLevel,
  SolstreamConfig,
  SubscribeUpdate,
} from '../src';

async function main() {
  const config: SolstreamConfig = {
    endpoint: process.env['SOLSTREAM_ENDPOINT'] ?? 'https://stream.example.com',
    apiKey: process.env['SOLSTREAM_API_KEY'],
  };

  const stream = await subscribe(
    config,
    {
      slots: {
        all: {
          filterByCommitment: false,
        },
      },
      commitment: CommitmentLevel.PROCESSED,
    },
    async (update: SubscribeUpdate) => {
      if (!update.slot) return;

      const { slotInfo } = update.slot;
      console.log(
        `Slot ${slotInfo?.slot}  parent=${slotInfo?.parent ?? '-'}  status=${slotInfo?.status}`,
      );
    },
    (error: Error) => {
      console.error('Stream error:', error.message);
    },
  );

  console.log(`Slot stream started (id=${stream.id}). Press Ctrl+C to stop.`);

  process.on('SIGINT', () => {
    console.log('\nCancelling stream…');
    stream.cancel();
    process.exit(0);
  });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
