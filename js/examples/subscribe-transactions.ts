/**
 * Example: Subscribe to non-vote transaction updates
 *
 * Set the following environment variables before running:
 *   SOLSTREAM_ENDPOINT  - gRPC endpoint, e.g. https://stream.example.com
 *   SOLSTREAM_API_KEY   - Your API key
 *
 * Run:
 *   npx ts-node examples/subscribe-transactions.ts
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
    replay: false,
  };

  const stream = await subscribe(
    config,
    {
      transactions: {
        'non-vote': {
          vote: false,
          failed: false,
          accountInclude: [],
          accountExclude: [],
          accountRequired: [],
        },
      },
      commitment: CommitmentLevel.PROCESSED,
    },
    async (update: SubscribeUpdate) => {
      if (!update.transaction) return;

      const { transaction, slot } = update.transaction;
      const sig = transaction?.signature
        ? Buffer.from(transaction.signature).toString('base64')
        : '<unknown>';

      const logCount = transaction?.meta?.logMessages?.length ?? 0;
      const fee = transaction?.meta?.fee ?? BigInt(0);

      console.log(
        `[slot ${slot}] tx ${sig.slice(0, 20)}…`,
        `fee=${fee}`,
        `logs=${logCount}`,
      );
    },
    (error: Error) => {
      console.error('Stream error:', error.message);
    },
  );

  console.log(`Stream started (id=${stream.id}). Press Ctrl+C to stop.`);

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
