/**
 * Example: Subscribe to full block updates
 *
 * Set the following environment variables before running:
 *   SOLSTREAM_ENDPOINT  - gRPC endpoint, e.g. https://stream.example.com
 *   SOLSTREAM_API_KEY   - Your API key
 *
 * Run:
 *   npx ts-node examples/subscribe-blocks.ts
 */

import {
  subscribeBlocks,
  SolstreamConfig,
  SubscribeBlockUpdate,
} from '../src';

async function main() {
  const config: SolstreamConfig = {
    endpoint: process.env['SOLSTREAM_ENDPOINT'] ?? 'https://stream.example.com',
    apiKey: process.env['SOLSTREAM_API_KEY'],
    replay: true,
  };

  const stream = await subscribeBlocks(
    config,
    {
      includeTransactions: true,
      includeAccounts: false,
      includeEntries: false,
    },
    async (update: SubscribeBlockUpdate) => {
      const slot = update.block?.slot ?? BigInt(0);
      const blockhash = update.block?.blockhash ?? '<unknown>';
      const txCount = update.transactions.length;
      const accountCount = update.updatedAccountCount;

      console.log(
        `Block slot=${slot}  hash=${blockhash.slice(0, 12)}…`,
        `txns=${txCount}  updatedAccounts=${accountCount}`,
        update.createdAt ? `at=${update.createdAt.toISOString()}` : '',
      );
    },
    (error: Error) => {
      console.error('Stream error:', error.message);
    },
  );

  console.log(`Block stream started (id=${stream.id}). Press Ctrl+C to stop.`);

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
