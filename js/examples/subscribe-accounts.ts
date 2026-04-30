/**
 * Example: Track SOL balance changes for a specific wallet.
 *
 * Mirrors the Rust SDK track_wallet example. Filters to a single account
 * address and prints lamport deltas as the balance changes.
 *
 * Set the following environment variables before running:
 *   SOLSTREAM_ENDPOINT  - gRPC endpoint, e.g. https://stream.example.com
 *   SOLSTREAM_API_KEY   - Your API key
 *   WALLET              - Base-58 wallet address to track (optional)
 *
 * Run:
 *   npx ts-node examples/subscribe-accounts.ts
 */

import { SolstreamClient, CommitmentLevel } from '../src';
import type { AccountUpdate } from '../src';

const WRAPPED_SOL = 'So11111111111111111111111111111111111111112';

async function main() {
  const endpoint = process.env['SOLSTREAM_ENDPOINT'] ?? 'https://stream.example.com';
  const apiKey   = process.env['SOLSTREAM_API_KEY'] ?? '';
  const wallet   = process.env['WALLET'] ?? WRAPPED_SOL;

  const client = SolstreamClient.connect(endpoint, apiKey);

  console.log(`Tracking wallet: ${wallet}`);

  const stream = client
    .messages()
    .accounts('wallet')
      .account(wallet)
      .build()
    .commitment(CommitmentLevel.CONFIRMED)
    .subscribe();

  process.on('SIGINT', () => {
    console.log('\nCancelling…');
    stream.cancel();
    process.exit(0);
  });

  let prevLamports: bigint | undefined;

  while (true) {
    const update = await stream.next();
    if (!update) break;
    const payload = update.decode();
    if (payload.kind === 'account') {
      printBalanceChange(payload.data, prevLamports);
      prevLamports = payload.data.lamports;
    }
  }
}

function printBalanceChange(acc: AccountUpdate, prev: bigint | undefined): void {
  const sol = Number(acc.lamports) / 1e9;
  if (prev === undefined) {
    console.log(`[slot ${acc.slot}]  ${sol.toFixed(9)} SOL  (initial)`);
  } else if (prev !== acc.lamports) {
    const delta = Number(acc.lamports - prev) / 1e9;
    const sign = delta >= 0 ? '+' : '';
    console.log(`[slot ${acc.slot}]  ${sol.toFixed(9)} SOL  (${sign}${delta.toFixed(9)} SOL)`);
  }
  // write_version bump with no balance change: skip
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
