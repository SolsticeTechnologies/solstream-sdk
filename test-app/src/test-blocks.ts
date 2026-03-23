/**
 * Test: Block updates
 *
 *   npm run test:blocks
 *
 * Subscribes to full block updates (with transactions).
 * Stops after TEST_DURATION_SECS seconds (default 30).
 */

import { subscribeBlocks, SubscribeBlockUpdate } from '@solstice/solstream-sdk';
import { config, TEST_DURATION_MS } from './config';
import { banner, info, success, error, stat, separator, lamportsToSol } from './logger';

async function main() {
  banner('TEST: Block Updates');
  info('CONFIG', `endpoint=${config.endpoint}`);
  info('CONFIG', `duration=${TEST_DURATION_MS / 1000}s  includeTransactions=true`);
  separator();

  let blockCount = 0;
  let totalTxns = 0;
  let totalFees = BigInt(0);

  const stream = await subscribeBlocks(
    config,
    {
      includeTransactions: true,
      includeAccounts: false,
      includeEntries: false,
    },
    async (update: SubscribeBlockUpdate) => {
      blockCount++;
      const slot = update.block?.slot ?? BigInt(0);
      const blockhash = update.block?.blockhash?.slice(0, 12) ?? '???';
      const txCount = update.transactions.length;
      const blockTime = update.block?.blockTime;
      const blockHeight = update.block?.blockHeight;

      totalTxns += txCount;

      // Sum fees from this block
      let blockFees = BigInt(0);
      for (const tx of update.transactions) {
        blockFees += tx.meta?.fee ?? BigInt(0);
      }
      totalFees += blockFees;

      success(
        'BLOCK',
        `slot=${slot}  hash=${blockhash}…  ` +
        `txns=${txCount}  fees=${lamportsToSol(blockFees)}` +
        (blockHeight !== undefined ? `  height=${blockHeight}` : '') +
        (blockTime !== undefined ? `  time=${new Date(Number(blockTime) * 1000).toISOString().slice(11, 19)}` : ''),
      );
    },
    (err: Error) => {
      error('STREAM', err.message);
    },
  );

  info('STREAM', `started  id=${stream.id}`);

  const finish = () => {
    stream.cancel();
    separator();
    stat('Blocks received', blockCount);
    stat('Total transactions', totalTxns);
    stat('Total fees', lamportsToSol(totalFees));
    if (blockCount > 0) {
      stat('Avg txns/block', (totalTxns / blockCount).toFixed(1));
    }
    info('DONE', 'Test complete.');
    process.exit(0);
  };

  setTimeout(finish, TEST_DURATION_MS);
  process.on('SIGINT', finish);
}

main().catch((err) => {
  error('FATAL', err.message);
  process.exit(1);
});
