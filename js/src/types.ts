/**
 * Solstream SDK — TypeScript type definitions
 * Mirrors the streaming.proto message structures.
 */

import type { ChannelOptions } from '@grpc/grpc-js';

// ─────────────────────────────────────────────────────────────────────────────
// Enums
// ─────────────────────────────────────────────────────────────────────────────

export enum CommitmentLevel {
  PROCESSED = 0,
  CONFIRMED = 1,
  FINALIZED = 2,
}

export enum SlotStatus {
  Unknown = 0,
  Processed = 1,
  Rooted = 2,
  Confirmed = 3,
  FirstShredReceived = 4,
  Completed = 5,
  CreatedBank = 6,
  Dead = 7,
}

export enum RewardType {
  Unspecified = 0,
  Fee = 1,
  Rent = 2,
  Staking = 3,
  Voting = 4,
}

// ─────────────────────────────────────────────────────────────────────────────
// SDK Configuration
// ─────────────────────────────────────────────────────────────────────────────

export interface SolstreamConfig {
  /** gRPC endpoint URL, e.g. "https://stream.example.com" */
  endpoint: string;
  /** Optional API key sent as gRPC metadata */
  apiKey?: string;
  /** Maximum reconnect attempts before giving up (default: unlimited) */
  maxReconnectAttempts?: number;
  /** Delay in ms between the first and second reconnect attempt (default: 1000) */
  baseReconnectDelayMs?: number;
  /** Maximum delay in ms between reconnect attempts (default: 30000) */
  maxReconnectDelayMs?: number;
  /** Additional @grpc/grpc-js channel options */
  channelOptions?: ChannelOptions;
  /**
   * When true, the client tracks the last processed slot and requests replay
   * from that slot on reconnect.  When false (default), each reconnect starts
   * from the current head.
   */
  replay?: boolean;
}

// ─────────────────────────────────────────────────────────────────────────────
// Subscribe request filters
// ─────────────────────────────────────────────────────────────────────────────

export interface SubscribeRequestFilterAccountsFilterMemcmp {
  offset: bigint | number;
  bytes?: Uint8Array;
  base58?: string;
  base64?: string;
}

export interface SubscribeRequestFilterAccountsFilterLamports {
  eq?: bigint | number;
  ne?: bigint | number;
  lt?: bigint | number;
  gt?: bigint | number;
}

export interface SubscribeRequestFilterAccountsFilter {
  memcmp?: SubscribeRequestFilterAccountsFilterMemcmp;
  datasize?: bigint | number;
  tokenAccountState?: boolean;
  lamports?: SubscribeRequestFilterAccountsFilterLamports;
}

export interface SubscribeRequestFilterAccounts {
  account?: string[];
  owner?: string[];
  filters?: SubscribeRequestFilterAccountsFilter[];
  nonemptyTxnSignature?: boolean;
}

export interface SubscribeRequestFilterSlots {
  filterByCommitment?: boolean;
}

export interface SubscribeRequestFilterTransactions {
  vote?: boolean;
  failed?: boolean;
  signature?: string;
  accountInclude?: string[];
  accountExclude?: string[];
  accountRequired?: string[];
}

export interface SubscribeRequestAccountsDataSlice {
  offset: bigint | number;
  length: bigint | number;
}

export interface SubscribeRequest {
  clientId?: string;
  accounts?: Record<string, SubscribeRequestFilterAccounts>;
  slots?: Record<string, SubscribeRequestFilterSlots>;
  transactions?: Record<string, SubscribeRequestFilterTransactions>;
  commitment?: CommitmentLevel;
  fromSlot?: bigint | number;
  includeBlockInfos?: boolean;
  includeEntries?: boolean;
  accountsDataSlice?: SubscribeRequestAccountsDataSlice[];
}

export interface SubscribeBlockRequest {
  clientId?: string;
  accountInclude?: string[];
  includeTransactions?: boolean;
  includeAccounts?: boolean;
  includeEntries?: boolean;
  fromSlot?: bigint | number;
}

// ─────────────────────────────────────────────────────────────────────────────
// Core types
// ─────────────────────────────────────────────────────────────────────────────

export interface AccountInfo {
  pubkey: Uint8Array;
  slot: bigint;
  lamports: bigint;
  owner: Uint8Array;
  executable: boolean;
  rentEpoch: bigint;
  data: Uint8Array;
  writeVersion: bigint;
  isStartup: boolean;
  version: number;
}

export interface SlotInfo {
  slot: bigint;
  parent?: bigint;
  status: SlotStatus;
}

export interface TransactionError {
  err: Uint8Array;
}

export interface MessageHeader {
  numRequiredSignatures: number;
  numReadonlySignedAccounts: number;
  numReadonlyUnsignedAccounts: number;
}

export interface MessageAddressTableLookup {
  accountKey: Uint8Array;
  writableIndexes: Uint8Array;
  readonlyIndexes: Uint8Array;
}

export interface CompiledInstruction {
  programIdIndex: number;
  accounts: Uint8Array;
  data: Uint8Array;
}

export interface Message {
  header?: MessageHeader;
  accountKeys: Uint8Array[];
  recentBlockhash: Uint8Array;
  instructions: CompiledInstruction[];
  versioned: boolean;
  addressTableLookups: MessageAddressTableLookup[];
}

export interface Transaction {
  signatures: Uint8Array[];
  message?: Message;
}

export interface InnerInstruction {
  instruction?: CompiledInstruction;
  stackHeight?: number;
}

export interface InnerInstructions {
  index: number;
  instructions: InnerInstruction[];
}

export interface ReturnData {
  programId: Uint8Array;
  data: Uint8Array;
}

export interface UiTokenAmount {
  uiAmount?: number;
  decimals: number;
  amount: string;
  uiAmountString: string;
}

export interface TokenBalance {
  accountIndex: number;
  mint: string;
  uiTokenAmount?: UiTokenAmount;
  owner: string;
  programId: string;
}

export interface Reward {
  pubkey: string;
  lamports: bigint;
  postBalance: bigint;
  rewardType: RewardType;
  commission?: number;
}

export interface TransactionStatusMeta {
  err?: TransactionError;
  fee: bigint;
  preBalances: bigint[];
  postBalances: bigint[];
  innerInstructions: InnerInstructions[];
  logMessages: string[];
  preTokenBalances: TokenBalance[];
  postTokenBalances: TokenBalance[];
  rewards: Reward[];
  loadedWritableAddresses: Uint8Array[];
  loadedReadonlyAddresses: Uint8Array[];
  returnData?: ReturnData;
  computeUnitsConsumed?: bigint;
  costUnits?: bigint;
}

export interface TransactionInfo {
  signature: Uint8Array;
  slot: bigint;
  messageHash: Uint8Array;
  isVote: boolean;
  transaction?: Transaction;
  transactionMeta?: TransactionStatusMeta;
  index?: bigint;
}

export interface BlockInfo {
  slot: bigint;
  parentSlot?: bigint;
  parentBlockhash?: string;
  blockhash: string;
  rewards: Reward[];
  blockTime?: bigint;
  blockHeight?: bigint;
  executedTransactionCount?: bigint;
  entryCount?: bigint;
  version: number;
}

export interface EntryInfo {
  slot: bigint;
  index: bigint;
  numHashes: bigint;
  hash: Uint8Array;
  executedTransactionCount: bigint;
  startingTransactionIndex: bigint;
}

// ─────────────────────────────────────────────────────────────────────────────
// Subscribe update variants
// ─────────────────────────────────────────────────────────────────────────────

export interface SubscribeUpdateAccount {
  account?: AccountInfo;
  slot: bigint;
  isStartup: boolean;
}

export interface SubscribeUpdateSlot {
  slotInfo?: SlotInfo;
}

export interface SubscribeUpdateTransaction {
  transaction?: TransactionInfo;
  slot: bigint;
}

export interface SubscribeUpdateBlockMeta {
  blockInfo?: BlockInfo;
}

export interface SubscribeUpdateEntry {
  entry?: EntryInfo;
}

export interface SubscribeUpdate {
  filters: string[];
  account?: SubscribeUpdateAccount;
  slot?: SubscribeUpdateSlot;
  transaction?: SubscribeUpdateTransaction;
  blockMeta?: SubscribeUpdateBlockMeta;
  entry?: SubscribeUpdateEntry;
  createdAt?: Date;
}

export interface SubscribeBlockUpdate {
  block?: BlockInfo;
  updatedAccountCount: bigint;
  accounts: AccountInfo[];
  transactions: TransactionInfo[];
  entries: EntryInfo[];
  createdAt?: Date;
}

// ─────────────────────────────────────────────────────────────────────────────
// Stream handle
// ─────────────────────────────────────────────────────────────────────────────

export interface StreamHandle {
  /** Unique stream identifier */
  id: string;
  /** Cancel the stream and stop reconnection attempts */
  cancel(): void;
  /** Dynamically update the subscription request on an active stream */
  write(request: SubscribeRequest): Promise<void>;
}
