/**
 * Solstream SDK
 *
 * High-performance gRPC client for real-time Solana data streaming.
 *
 * @example
 * ```ts
 * import { subscribe, CommitmentLevel } from '@solstice/solstream-sdk';
 *
 * const stream = await subscribe(
 *   { endpoint: 'https://stream.example.com', apiKey: 'YOUR_KEY' },
 *   {
 *     accounts: { all: { account: [], owner: [], filters: [] } },
 *     commitment: CommitmentLevel.CONFIRMED,
 *   },
 *   (update) => console.log(update),
 * );
 *
 * process.on('SIGINT', () => stream.cancel());
 * ```
 */

export { subscribe, subscribeBlocks } from './client';

export {
  // Enums
  CommitmentLevel,
  SlotStatus,
  RewardType,
  // Config
  SolstreamConfig,
  // Request types
  SubscribeRequest,
  SubscribeBlockRequest,
  SubscribeRequestFilterAccounts,
  SubscribeRequestFilterAccountsFilter,
  SubscribeRequestFilterAccountsFilterMemcmp,
  SubscribeRequestFilterAccountsFilterLamports,
  SubscribeRequestFilterSlots,
  SubscribeRequestFilterTransactions,
  SubscribeRequestAccountsDataSlice,
  // Update types
  SubscribeUpdate,
  SubscribeUpdateAccount,
  SubscribeUpdateSlot,
  SubscribeUpdateTransaction,
  SubscribeUpdateBlockMeta,
  SubscribeUpdateEntry,
  SubscribeBlockUpdate,
  // Stream
  StreamHandle,
  // Geyser types
  AccountInfo,
  SlotInfo,
  TransactionInfo,
  BlockInfo,
  EntryInfo,
  Transaction,
  Message,
  MessageHeader,
  MessageAddressTableLookup,
  CompiledInstruction,
  InnerInstruction,
  InnerInstructions,
  TransactionStatusMeta,
  TransactionError,
  ReturnData,
  TokenBalance,
  UiTokenAmount,
  Reward,
} from './types';
