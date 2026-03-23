/**
 * Example: Subscribe to accounts with advanced filters
 *
 * Demonstrates:
 *   - Filtering by data size
 *   - Filtering by lamport balance
 *   - Filtering by memory comparison (memcmp)
 *
 * Set the following environment variables before running:
 *   SOLSTREAM_ENDPOINT  - gRPC endpoint
 *   SOLSTREAM_API_KEY   - Your API key
 *
 * Run:
 *   npx ts-node examples/subscribe-filtered-accounts.ts
 */

import {
  subscribe,
  CommitmentLevel,
  SolstreamConfig,
  SubscribeUpdate,
} from '../src';

// Stake program
const STAKE_PROGRAM = 'Stake11111111111111111111111111111111111111';

async function main() {
  const config: SolstreamConfig = {
    endpoint: process.env['SOLSTREAM_ENDPOINT'] ?? 'https://stream.example.com',
    apiKey: process.env['SOLSTREAM_API_KEY'],
    replay: true,
    maxReconnectAttempts: 10,
    baseReconnectDelayMs: 500,
    maxReconnectDelayMs: 15_000,
  };

  const stream = await subscribe(
    config,
    {
      accounts: {
        // Only stake accounts with > 1 SOL
        'funded-stake-accounts': {
          account: [],
          owner: [STAKE_PROGRAM],
          filters: [
            {
              lamports: { gt: 1_000_000_000 }, // > 1 SOL in lamports
            },
          ],
        },
      },
      commitment: CommitmentLevel.FINALIZED,
    },
    async (update: SubscribeUpdate) => {
      if (!update.account) return;

      const { account, slot } = update.account;
      const pubkey = account?.pubkey
        ? Buffer.from(account.pubkey).toString('base64')
        : '<unknown>';

      console.log(
        `[slot ${slot}] Stake account ${pubkey}`,
        `lamports=${account?.lamports}`,
        `dataLen=${account?.data.length ?? 0}`,
      );
    },
    (error: Error) => {
      console.error('Stream error:', error.message);
    },
  );

  console.log(`Filtered account stream started (id=${stream.id}). Press Ctrl+C to stop.`);

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
