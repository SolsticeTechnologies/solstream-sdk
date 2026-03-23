/**
 * Example: Subscribe to account updates
 *
 * Set the following environment variables before running:
 *   SOLSTREAM_ENDPOINT  - gRPC endpoint, e.g. https://stream.example.com
 *   SOLSTREAM_API_KEY   - Your API key
 *
 * Run:
 *   npx ts-node examples/subscribe-accounts.ts
 */

import {
  subscribe,
  CommitmentLevel,
  SolstreamConfig,
  SubscribeUpdate,
} from '../src';

const TOKEN_PROGRAM = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA';

async function main() {
  const config: SolstreamConfig = {
    endpoint: process.env['SOLSTREAM_ENDPOINT'] ?? 'https://stream.example.com',
    apiKey: process.env['SOLSTREAM_API_KEY'],
    replay: true,
  };

  const stream = await subscribe(
    config,
    {
      accounts: {
        'token-accounts': {
          account: [],
          owner: [TOKEN_PROGRAM],
          filters: [],
        },
      },
      commitment: CommitmentLevel.CONFIRMED,
    },
    async (update: SubscribeUpdate) => {
      if (!update.account) return;

      const { account, slot, isStartup } = update.account;
      const pubkey = account?.pubkey
        ? Buffer.from(account.pubkey).toString('base64')
        : '<unknown>';

      console.log(
        `[slot ${slot}] Account ${pubkey}`,
        `lamports=${account?.lamports}`,
        isStartup ? '(startup)' : '',
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
