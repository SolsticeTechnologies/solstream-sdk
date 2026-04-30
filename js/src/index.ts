/**
 * Solstream SDK
 *
 * ## Primary API (recommended)
 *
 * ```ts
 * import { SolstreamClient, CommitmentLevel } from '@solstice/solstream-sdk';
 *
 * const client = SolstreamClient.connect('https://stream.example.com', 'YOUR_KEY');
 *
 * const stream = client
 *   .messages()
 *   .transactions('token-txns')
 *     .includeAccount('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA')
 *     .excludeVotes()
 *     .excludeFailed()
 *     .build()
 *   .commitment(CommitmentLevel.CONFIRMED)
 *   .subscribe();
 *
 * while (true) {
 *   const update = await stream.next();
 *   if (!update) break;
 *   const payload = update.decode();
 *   if (payload.kind === 'transaction') {
 *     console.log(payload.data.signature);  // base-58 string — no manual encoding
 *   }
 * }
 * ```
 *
 * ## Legacy callback API
 *
 * ```ts
 * import { subscribe, CommitmentLevel } from '@solstice/solstream-sdk';
 *
 * const stream = await subscribe(
 *   { endpoint: 'https://stream.example.com', apiKey: 'YOUR_KEY' },
 *   { slots: { all: {} }, commitment: CommitmentLevel.CONFIRMED },
 *   (update) => console.log(update),
 * );
 * process.on('SIGINT', () => stream.cancel());
 * ```
 */

// ── Primary API ──────────────────────────────────────────────────────────────
export { SolstreamClient } from './client';
export { RequestBuilder, BlockRequestBuilder, AccountFilterBuilder, TransactionFilterBuilder, SlotFilterBuilder } from './builder';
export { MessageStream, BlockStream } from './stream';
export { ClientError } from './error';
export type { ClientErrorKind } from './error';

// ── Decoded types (returned by MessageStream / BlockStream) ──────────────────
export type {
  ReconnectConfig,
  AccountUpdate,
  TransactionUpdate,
  SlotUpdate,
  EntryUpdate,
  BlockInfo,
  BlockUpdate,
  Instruction,
  InnerInstructionDecoded,
  InnerInstructionsDecoded,
  TokenBalanceDecoded,
  RewardDecoded,
  StreamUpdate,
  UpdatePayload,
} from './decoded-types';

// ── Legacy callback API ──────────────────────────────────────────────────────
export { subscribe, subscribeBlocks } from './client';

// ── Enums (shared) ───────────────────────────────────────────────────────────
export { CommitmentLevel, SlotStatus, RewardType } from './types';

// ── Legacy / raw proto types ─────────────────────────────────────────────────
export type {
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
  // Raw update types (from legacy subscribe())
  SubscribeUpdate,
  SubscribeUpdateAccount,
  SubscribeUpdateSlot,
  SubscribeUpdateTransaction,
  SubscribeUpdateBlockMeta,
  SubscribeUpdateEntry,
  SubscribeBlockUpdate,
  // Raw stream handle (from legacy subscribe())
  StreamHandle,
  // Raw proto types
  AccountInfo,
  SlotInfo,
  TransactionInfo,
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
