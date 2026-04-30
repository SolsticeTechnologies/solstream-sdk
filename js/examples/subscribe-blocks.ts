/**
 * Example: Subscribe to full block updates.
 *
 * Mirrors the Rust SDK full_blocks example. Delivers one message per finalized
 * block containing the header, all transactions, and all account state changes.
 *
 * Note: enabling withAccounts(true) on busy blocks can produce large messages.
 * Compression is negotiated automatically by the client (gzip by default).
 *
 * Set the following environment variables before running:
 *   SOLSTREAM_ENDPOINT  - gRPC endpoint, e.g. https://stream.example.com
 *   SOLSTREAM_API_KEY   - Your API key
 *
 * Run:
 *   npx ts-node examples/subscribe-blocks.ts
 */

import { SolstreamClient } from '../src';
import type { BlockUpdate } from '../src';

async function main() {
  const endpoint = process.env['SOLSTREAM_ENDPOINT'] ?? 'https://stream.example.com';
  const apiKey   = process.env['SOLSTREAM_API_KEY'] ?? '';

  const client = SolstreamClient.connect(endpoint, apiKey);

  console.log('Subscribing to full blocks (transactions + accounts)…\n');

  const stream = client
    .blocks()
    .withTransactions(true)
    .withAccounts(true)
    .withEntries(false)
    .subscribe();

  process.on('SIGINT', () => {
    console.log('\nCancelling…');
    stream.cancel();
    process.exit(0);
  });

  while (true) {
    const block = await stream.next();
    if (!block) break;
    printBlock(block);
  }
}

function printBlock(b: BlockUpdate): void {
  const info = b.block;

  console.log(
    `┌─ slot=${info.slot} height=${info.blockHeight ?? 0n} hash=${info.blockhash.slice(0, 16)}`,
  );
  console.log(
    `│  transactions=${b.transactions.length}  accounts=${b.accounts.length}  rewards=${info.rewards.length}`,
  );

  for (const tx of b.transactions.slice(0, 5)) {
    console.log(
      `│    tx ${tx.signature.slice(0, 20)} ok=${tx.success} fee=${tx.fee} cu=${tx.computeUnitsConsumed ?? 0n}`,
    );
  }
  if (b.transactions.length > 5) {
    console.log(`│    … and ${b.transactions.length - 5} more transactions`);
  }

  const topAccounts = [...b.accounts].sort((a, c) => (c.lamports > a.lamports ? 1 : -1)).slice(0, 3);
  for (const acc of topAccounts) {
    console.log(`│    account ${acc.pubkey.slice(0, 20)} lamports=${acc.lamports}`);
  }
  if (b.accounts.length > 3) {
    console.log(`│    … and ${b.accounts.length - 3} more accounts`);
  }

  console.log('└─');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
