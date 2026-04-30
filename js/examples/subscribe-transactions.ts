/**
 * Example: Monitor successful non-vote transactions for a program.
 *
 * Mirrors the Rust SDK monitor_program example. Uses a transaction filter with
 * includeAccount, excludeVotes, and excludeFailed. Also layers on a slot filter
 * to show commitment progression.
 *
 * Defaults to Jupiter V6. Override with the PROGRAM env var.
 *
 * Set the following environment variables before running:
 *   SOLSTREAM_ENDPOINT  - gRPC endpoint, e.g. https://stream.example.com
 *   SOLSTREAM_API_KEY   - Your API key
 *   PROGRAM             - Program address to monitor (optional)
 *
 * Run:
 *   npx ts-node examples/subscribe-transactions.ts
 */

import { SolstreamClient, CommitmentLevel, SlotStatus } from '../src';
import type { TransactionUpdate } from '../src';

const JUPITER_V6 = 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4';

async function main() {
  const endpoint = process.env['SOLSTREAM_ENDPOINT'] ?? 'https://stream.example.com';
  const apiKey   = process.env['SOLSTREAM_API_KEY'] ?? '';
  const program  = process.env['PROGRAM'] ?? JUPITER_V6;

  const client = SolstreamClient.connect(endpoint, apiKey);

  console.log(`Monitoring program: ${program}`);
  console.log(
    'signature'.padEnd(90),
    'slot'.padStart(10),
    'fee'.padStart(10),
    'cu'.padStart(8),
  );

  const stream = client
    .messages()
    .transactions('program-txs')
      .includeAccount(program)
      .excludeVotes()
      .excludeFailed()
      .build()
    .slots('slots')
      .filterByCommitment(true)
      .build()
    .commitment(CommitmentLevel.CONFIRMED)
    .subscribe();

  process.on('SIGINT', () => {
    console.log('\nCancelling…');
    stream.cancel();
    process.exit(0);
  });

  while (true) {
    const update = await stream.next();
    if (!update) break;
    const payload = update.decode();
    if (payload.kind === 'transaction') {
      printTx(payload.data);
    } else if (payload.kind === 'slot' && payload.data.status === SlotStatus.Confirmed) {
      console.log(`--- confirmed slot ${payload.data.slot} ---`);
    }
  }
}

function printTx(tx: TransactionUpdate): void {
  console.log(
    tx.signature.padEnd(90),
    String(tx.slot).padStart(10),
    String(tx.fee).padStart(10),
    String(tx.computeUnitsConsumed ?? 0n).padStart(8),
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
