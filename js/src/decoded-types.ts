/**
 * Decoded type interfaces — mirrors the Rust SDK's types.rs.
 *
 * All binary fields that were raw bytes in the proto are decoded here:
 *   - Account / owner / signature / blockhash → base-58 strings
 *   - Account keys list → base-58 string[]
 *
 * Compare to the raw proto types in types.ts which are still available
 * for callers that need direct access to the wire format.
 */

export { CommitmentLevel, RewardType, SlotStatus } from './types';

// ─────────────────────────────────────────────────────────────────────────────
// Reconnect configuration
// ─────────────────────────────────────────────────────────────────────────────

/** Policy controlling how streams reconnect after a failure. */
export interface ReconnectConfig {
  /**
   * Maximum consecutive reconnect attempts before surfacing an error.
   * `undefined` (default) means retry indefinitely.
   */
  maxAttempts?: number;
  /** Delay before the first reconnect attempt (default: 500 ms). */
  initialBackoffMs?: number;
  /** Upper bound on inter-attempt delay (default: 30 000 ms). */
  maxBackoffMs?: number;
  /**
   * Multiplier applied to the backoff after each failed attempt (default: 2.0).
   * Must be ≥ 1.0.
   */
  backoffFactor?: number;
}

// ─────────────────────────────────────────────────────────────────────────────
// Decoded update types
// ─────────────────────────────────────────────────────────────────────────────

/** Decoded account-state update. */
export interface AccountUpdate {
  /** Account address (base-58). */
  pubkey: string;
  slot: bigint;
  /** Lamport balance. */
  lamports: bigint;
  /** Owner program address (base-58). */
  owner: string;
  executable: boolean;
  rentEpoch: bigint;
  /** Raw account data bytes. */
  data: Uint8Array;
  writeVersion: bigint;
  /** `true` when delivered during the initial snapshot, not as a real-time change. */
  isStartup: boolean;
  /** Encode `data` as a standard base-64 string. */
  dataBase64(): string;
}

/** A single decoded compiled instruction. */
export interface Instruction {
  /** Index into the transaction's `accountKeys` array for the program. */
  programIdIdx: number;
  /** Packed byte array where each byte is an index into `accountKeys`. */
  accounts: Uint8Array;
  /** Raw instruction data. */
  data: Uint8Array;
  /** Encode `data` as base-64. */
  dataBase64(): string;
  /** Encode `data` as base-58. */
  dataBase58(): string;
}

/** A single CPI instruction. */
export interface InnerInstructionDecoded {
  /** Call-stack depth (absent for old validator versions). */
  stackHeight?: number;
  instruction: Instruction;
}

/** CPI instructions emitted by one top-level instruction. */
export interface InnerInstructionsDecoded {
  /** Index of the top-level instruction that produced these. */
  index: number;
  instructions: InnerInstructionDecoded[];
}

/** SPL-token balance for one account. */
export interface TokenBalanceDecoded {
  /** Index into the transaction's `accountKeys`. */
  accountIndex: number;
  mint: string;
  /** Raw on-chain integer string (e.g. `"1000000"`). */
  amount: string;
  /** Human-readable amount adjusted for decimals. */
  uiAmount?: number;
  decimals: number;
  /** Token account owner (base-58). */
  owner: string;
  /** Token program ID (base-58). */
  programId: string;
}

/** Decoded transaction update. */
export interface TransactionUpdate {
  /** Transaction signature (base-58). */
  signature: string;
  slot: bigint;
  isVote: boolean;
  /** Message hash (base-58). */
  messageHash: string;
  /**
   * All account addresses referenced by this transaction (base-58).
   * Includes static message keys + loaded addresses from lookup tables.
   * Instruction `programIdIdx` / `accounts` indices refer into this list.
   */
  accountKeys: string[];
  /** Recent blockhash (base-58). */
  recentBlockhash: string;
  instructions: Instruction[];
  innerInstructions: InnerInstructionsDecoded[];
  /** Transaction fee in lamports. */
  fee: bigint;
  /** `true` when the transaction succeeded (no error). */
  success: boolean;
  logMessages: string[];
  computeUnitsConsumed?: bigint;
  /** SOL balances (lamports) per account before execution. */
  preBalances: bigint[];
  /** SOL balances after execution. */
  postBalances: bigint[];
  preTokenBalances: TokenBalanceDecoded[];
  postTokenBalances: TokenBalanceDecoded[];
}

/** Decoded slot-status update. */
export interface SlotUpdate {
  slot: bigint;
  parent?: bigint;
  status: import('./types').SlotStatus;
}

/** Decoded ledger entry (PoH tick or transaction batch). */
export interface EntryUpdate {
  slot: bigint;
  index: bigint;
  numHashes: bigint;
  /** Raw PoH hash bytes. */
  hash: Uint8Array;
  executedTransactionCount: bigint;
  startingTransactionIndex: bigint;
  /** Encode the PoH `hash` as base-58. */
  hashBase58(): string;
}

/** A block reward distributed at finalisation. */
export interface RewardDecoded {
  /** Recipient address (base-58). */
  pubkey: string;
  lamports: bigint;
  postBalance: bigint;
  rewardType: import('./types').RewardType;
  /** Validator commission (only present for staking rewards). */
  commission?: number;
}

/** Metadata for a confirmed or finalised block. */
export interface BlockInfo {
  slot: bigint;
  blockhash: string;
  parentSlot?: bigint;
  parentBlockhash?: string;
  /** Unix timestamp (seconds since epoch) assigned by the leader. */
  blockTime?: bigint;
  blockHeight?: bigint;
  transactionCount?: bigint;
  entryCount?: bigint;
  rewards: RewardDecoded[];
}

/** Full decoded block update (from SubscribeBlocks). */
export interface BlockUpdate {
  block: BlockInfo;
  updatedAccountCount: bigint;
  /** Account states (populated when `withAccounts(true)` was set). */
  accounts: AccountUpdate[];
  /** Transactions (populated when `withTransactions(true)` was set). */
  transactions: TransactionUpdate[];
  /** Entries (populated when `withEntries(true)` was set). */
  entries: EntryUpdate[];
  createdAt?: Date;
}

// ─────────────────────────────────────────────────────────────────────────────
// UpdatePayload — discriminated union returned by StreamUpdate.decode()
// ─────────────────────────────────────────────────────────────────────────────

export type UpdatePayload =
  | { kind: 'account';     data: AccountUpdate }
  | { kind: 'transaction'; data: TransactionUpdate }
  | { kind: 'slot';        data: SlotUpdate }
  | { kind: 'blockMeta';   data: BlockInfo }
  | { kind: 'entry';       data: EntryUpdate };

/** Raw update wrapper returned by MessageStream.next(). Call .decode() for decoded types. */
export interface StreamUpdate {
  filters: string[];
  createdAt?: Date;
  /** Cheaply read the slot number without fully decoding the message. */
  slot(): bigint;
  /**
   * Decode the raw proto bytes into the ergonomic `UpdatePayload` type.
   * This is where base-58 encoding of addresses happens.
   */
  decode(): UpdatePayload;
}
