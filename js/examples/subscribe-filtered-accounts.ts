/**
 * Example: Subscribe to accounts with advanced filter predicates.
 *
 * Mirrors the Rust SDK advanced_filters example. Combines multiple named
 * account filters in a single subscription — the server evaluates them
 * independently and tags each update with the filter name(s) it matched.
 *
 * Filters:
 *   "spl-token-accounts" — SPL Token accounts (owned by token program, 165 B)
 *   "funded-wallets"     — System accounts with > 1 SOL
 *   "mint-disc"          — Accounts whose first byte is 0x01 (SPL Mint disc.)
 *
 * Set the following environment variables before running:
 *   SOLSTREAM_ENDPOINT  - gRPC endpoint, e.g. https://stream.example.com
 *   SOLSTREAM_API_KEY   - Your API key
 *
 * Run:
 *   npx ts-node examples/subscribe-filtered-accounts.ts
 */

import { SolstreamClient, CommitmentLevel } from '../src';
import type { ReconnectConfig } from '../src';

const TOKEN_PROGRAM  = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA';
const SYSTEM_PROGRAM = '11111111111111111111111111111111';
const ONE_SOL = 1_000_000_000n;

async function main() {
  const endpoint = process.env['SOLSTREAM_ENDPOINT'] ?? 'https://stream.example.com';
  const apiKey   = process.env['SOLSTREAM_API_KEY'] ?? '';

  const client = SolstreamClient.connect(endpoint, apiKey);

  const reconnect: ReconnectConfig = {
    maxAttempts: 10,
    initialBackoffMs: 500,
    maxBackoffMs: 15_000,
    backoffFactor: 2.0,
  };

  const stream = client
    .messages()
    // All SPL token accounts: owned by token program, exactly 165 bytes.
    .accounts('spl-token-accounts')
      .owner(TOKEN_PROGRAM)
      .dataSize(165)
      .tokenAccountState()
      .build()
    // System-owned accounts with more than 1 SOL.
    .accounts('funded-wallets')
      .owner(SYSTEM_PROGRAM)
      .lamportsGt(ONE_SOL)
      .build()
    // Accounts whose data starts with byte 0x01 — SPL Mint discriminator.
    .accounts('mint-disc')
      .owner(TOKEN_PROGRAM)
      .memcmp(0, new Uint8Array([0x01]))
      .build()
    .commitment(CommitmentLevel.CONFIRMED)
    .reconnect(reconnect)
    .subscribe();

  console.log(
    'filter'.padEnd(20),
    'pubkey'.padEnd(44),
    'lamports'.padStart(14),
    'data(B)'.padStart(9),
  );

  process.on('SIGINT', () => {
    console.log('\nCancelling…');
    stream.cancel();
    process.exit(0);
  });

  while (true) {
    const update = await stream.next();
    if (!update) break;
    const filters = update.filters.join(',');
    const payload = update.decode();
    if (payload.kind === 'account') {
      const acc = payload.data;
      console.log(
        filters.padEnd(20),
        acc.pubkey.padEnd(44),
        String(acc.lamports).padStart(14),
        String(acc.data.length).padStart(9),
      );
    }
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
