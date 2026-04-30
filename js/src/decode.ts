/**
 * Proto → decoded-type conversion layer.
 *
 * Mirrors the Rust SDK's decode.rs.  All public functions are used by
 * MessageStream / BlockStream; callers should never need to import these
 * directly.
 */

import bs58 from 'bs58';
import { ClientError } from './error';
import {
  AccountUpdate,
  BlockInfo,
  BlockUpdate,
  EntryUpdate,
  InnerInstructionDecoded,
  InnerInstructionsDecoded,
  Instruction,
  RewardDecoded,
  RewardType,
  SlotStatus,
  SlotUpdate,
  StreamUpdate,
  TokenBalanceDecoded,
  TransactionUpdate,
  UpdatePayload,
} from './decoded-types';

// ─────────────────────────────────────────────────────────────────────────────
// Utilities
// ─────────────────────────────────────────────────────────────────────────────

/** Encode raw bytes to a base-58 string.  Empty slice → empty string. */
function toBase58(bytes: Uint8Array | null | undefined): string {
  if (!bytes || bytes.length === 0) return '';
  return bs58.encode(bytes);
}

// ─────────────────────────────────────────────────────────────────────────────
// StreamUpdate wrapper
// ─────────────────────────────────────────────────────────────────────────────

/** Wrap a raw proto object (from grpc-js) in a StreamUpdate. Cheap — no decoding. */
export function wrapRawUpdate(raw: any): StreamUpdate {
  return {
    filters: raw.filters ?? [],
    createdAt: raw.createdAt ? new Date(Number(raw.createdAt.seconds) * 1000) : undefined,
    slot(): bigint {
      if (raw.transaction?.slot !== undefined) return BigInt(raw.transaction.slot);
      if (raw.account?.slot !== undefined) return BigInt(raw.account.slot);
      if (raw.slot?.slotInfo?.slot !== undefined) return BigInt(raw.slot.slotInfo.slot);
      if (raw.blockMeta?.blockInfo?.slot !== undefined) return BigInt(raw.blockMeta.blockInfo.slot);
      if (raw.entry?.entry?.slot !== undefined) return BigInt(raw.entry.entry.slot);
      return BigInt(0);
    },
    decode(): UpdatePayload {
      return decodeUpdate(raw);
    },
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// Top-level dispatcher
// ─────────────────────────────────────────────────────────────────────────────

export function decodeUpdate(raw: any): UpdatePayload {
  if (raw.account) return { kind: 'account', data: decodeAccountUpdateProto(raw.account) };
  if (raw.transaction) return { kind: 'transaction', data: decodeTransactionUpdateProto(raw.transaction, raw.transaction?.slot) };
  if (raw.slot) return { kind: 'slot', data: decodeSlotUpdateProto(raw.slot) };
  if (raw.blockMeta) return { kind: 'blockMeta', data: decodeBlockInfoProto(raw.blockMeta.blockInfo) };
  if (raw.entry) return { kind: 'entry', data: decodeEntryInfoProto(raw.entry.entry) };
  throw ClientError.decode('update envelope contained no payload');
}

export function decodeBlockUpdateProto(raw: any): BlockUpdate {
  const block = decodeBlockInfoProto(raw.block);
  const accounts = (raw.accounts ?? []).map((a: any) => decodeAccountInfoProto(a));
  const transactions = (raw.transactions ?? []).map((t: any) => decodeTransactionInfoProto(t));
  const entries = (raw.entries ?? []).map((e: any) => decodeEntryInfoProto(e));
  return {
    block,
    updatedAccountCount: BigInt(raw.updatedAccountCount ?? 0),
    accounts,
    transactions,
    entries,
    createdAt: raw.createdAt ? new Date(Number(raw.createdAt.seconds) * 1000) : undefined,
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// Account
// ─────────────────────────────────────────────────────────────────────────────

function decodeAccountUpdateProto(proto: any): AccountUpdate {
  const info = proto.account ?? proto;
  const decoded = decodeAccountInfoProto(info);
  // The outer SubscribeUpdateAccount.isStartup is authoritative
  if (proto.isStartup !== undefined) decoded.isStartup = proto.isStartup;
  decoded.slot = BigInt(proto.slot ?? info.slot ?? 0);
  return decoded;
}

export function decodeAccountInfoProto(proto: any): AccountUpdate {
  const pubkey = toBase58(proto.pubkey);
  const owner = toBase58(proto.owner);
  if (!pubkey) throw ClientError.decode('missing required field: pubkey');
  if (!owner) throw ClientError.decode('missing required field: owner');

  const data = makeData(proto.data);
  return {
    pubkey,
    slot: BigInt(proto.slot ?? 0),
    lamports: BigInt(proto.lamports ?? 0),
    owner,
    executable: proto.executable ?? false,
    rentEpoch: BigInt(proto.rentEpoch ?? 0),
    data,
    writeVersion: BigInt(proto.writeVersion ?? 0),
    isStartup: proto.isStartup ?? false,
    dataBase64(): string {
      return Buffer.from(data).toString('base64');
    },
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// Transaction
// ─────────────────────────────────────────────────────────────────────────────

function decodeTransactionUpdateProto(proto: any, slotOverride?: any): TransactionUpdate {
  const txInfo = proto.transaction ?? proto;
  return decodeTransactionInfoProto(txInfo, slotOverride ?? proto.slot);
}

export function decodeTransactionInfoProto(txInfo: any, slotOverride?: any): TransactionUpdate {
  const signature = toBase58(txInfo.signature);
  if (!signature) throw ClientError.decode('missing required field: signature');

  const messageHash = toBase58(txInfo.messageHash ?? txInfo.message_hash);

  // Build full account key list: message keys + loaded addresses from ALTs
  const rawKeys: Uint8Array[] = txInfo.transaction?.message?.accountKeys
    ?? txInfo.transaction?.message?.account_keys
    ?? [];
  const loadedWritable: Uint8Array[] = txInfo.transactionMeta?.loadedWritableAddresses
    ?? txInfo.transaction_meta?.loaded_writable_addresses
    ?? [];
  const loadedReadonly: Uint8Array[] = txInfo.transactionMeta?.loadedReadonlyAddresses
    ?? txInfo.transaction_meta?.loaded_readonly_addresses
    ?? [];

  const allRawKeys = [...rawKeys, ...loadedWritable, ...loadedReadonly];
  const accountKeys = allRawKeys.map(k => toBase58(k));

  const recentBlockhash = toBase58(
    txInfo.transaction?.message?.recentBlockhash
    ?? txInfo.transaction?.message?.recent_blockhash,
  );

  const instructions: Instruction[] = (
    txInfo.transaction?.message?.instructions ?? []
  ).map(decodeInstruction);

  const meta = txInfo.transactionMeta ?? txInfo.transaction_meta;

  const innerInstructions: InnerInstructionsDecoded[] = (meta?.innerInstructions ?? meta?.inner_instructions ?? [])
    .map(decodeInnerInstructions);

  const preTokenBalances: TokenBalanceDecoded[] = (meta?.preTokenBalances ?? meta?.pre_token_balances ?? [])
    .map(decodeTokenBalance);

  const postTokenBalances: TokenBalanceDecoded[] = (meta?.postTokenBalances ?? meta?.post_token_balances ?? [])
    .map(decodeTokenBalance);

  const fee = BigInt(meta?.fee ?? 0);
  const success = meta ? meta.err == null || (meta.err?.err?.length ?? 0) === 0 : false;
  const logMessages: string[] = meta?.logMessages ?? meta?.log_messages ?? [];
  const computeUnitsConsumed = meta?.computeUnitsConsumed !== undefined && meta.computeUnitsConsumed !== null
    ? BigInt(meta.computeUnitsConsumed)
    : meta?.compute_units_consumed !== undefined && meta.compute_units_consumed !== null
      ? BigInt(meta.compute_units_consumed)
      : undefined;
  const preBalances = (meta?.preBalances ?? meta?.pre_balances ?? []).map(BigInt);
  const postBalances = (meta?.postBalances ?? meta?.post_balances ?? []).map(BigInt);

  return {
    signature,
    slot: BigInt(slotOverride ?? txInfo.slot ?? 0),
    isVote: txInfo.isVote ?? txInfo.is_vote ?? false,
    messageHash,
    accountKeys,
    recentBlockhash,
    instructions,
    innerInstructions,
    fee,
    success,
    logMessages,
    computeUnitsConsumed,
    preBalances,
    postBalances,
    preTokenBalances,
    postTokenBalances,
  };
}

function decodeInstruction(ix: any): Instruction {
  const data = makeData(ix.data);
  const accounts = makeData(ix.accounts);
  return {
    programIdIdx: ix.programIdIndex ?? ix.program_id_index ?? 0,
    accounts,
    data,
    dataBase64(): string { return Buffer.from(data).toString('base64'); },
    dataBase58(): string { return toBase58(data); },
  };
}

function decodeInnerInstructions(ii: any): InnerInstructionsDecoded {
  const instructions: InnerInstructionDecoded[] = (ii.instructions ?? []).map((inner: any) => {
    const ix = inner.instruction;
    if (!ix) throw ClientError.decode('missing required field: inner instruction');
    return {
      stackHeight: inner.stackHeight ?? inner.stack_height,
      instruction: decodeInstruction(ix),
    };
  });
  return { index: ii.index ?? 0, instructions };
}

function decodeTokenBalance(tb: any): TokenBalanceDecoded {
  const ui = tb.uiTokenAmount ?? tb.ui_token_amount ?? {};
  return {
    accountIndex: tb.accountIndex ?? tb.account_index ?? 0,
    mint: tb.mint ?? '',
    amount: ui.amount ?? '',
    uiAmount: ui.uiAmount ?? ui.ui_amount,
    decimals: ui.decimals ?? 0,
    owner: tb.owner ?? '',
    programId: tb.programId ?? tb.program_id ?? '',
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// Block / BlockInfo
// ─────────────────────────────────────────────────────────────────────────────

export function decodeBlockInfoProto(proto: any): BlockInfo {
  if (!proto) throw ClientError.decode('missing required field: block_info');
  return {
    slot: BigInt(proto.slot ?? 0),
    blockhash: proto.blockhash ?? '',
    parentSlot: proto.parentSlot !== undefined ? BigInt(proto.parentSlot) : proto.parent_slot !== undefined ? BigInt(proto.parent_slot) : undefined,
    parentBlockhash: proto.parentBlockhash ?? proto.parent_blockhash,
    blockTime: proto.blockTime !== undefined ? BigInt(proto.blockTime) : proto.block_time !== undefined ? BigInt(proto.block_time) : undefined,
    blockHeight: proto.blockHeight !== undefined ? BigInt(proto.blockHeight) : proto.block_height !== undefined ? BigInt(proto.block_height) : undefined,
    transactionCount: proto.executedTransactionCount !== undefined ? BigInt(proto.executedTransactionCount) : proto.executed_transaction_count !== undefined ? BigInt(proto.executed_transaction_count) : undefined,
    entryCount: proto.entryCount !== undefined ? BigInt(proto.entryCount) : proto.entry_count !== undefined ? BigInt(proto.entry_count) : undefined,
    rewards: (proto.rewards ?? []).map(decodeReward),
  };
}

function decodeReward(r: any): RewardDecoded {
  return {
    pubkey: r.pubkey ?? '',
    lamports: BigInt(r.lamports ?? 0),
    postBalance: BigInt(r.postBalance ?? r.post_balance ?? 0),
    rewardType: decodeRewardType(r.rewardType ?? r.reward_type),
    commission: r.commission,
  };
}

function decodeRewardType(rt: number | string | undefined): RewardType {
  if (rt === 1 || rt === 'Fee') return RewardType.Fee;
  if (rt === 2 || rt === 'Rent') return RewardType.Rent;
  if (rt === 3 || rt === 'Staking') return RewardType.Staking;
  if (rt === 4 || rt === 'Voting') return RewardType.Voting;
  return RewardType.Unspecified;
}

// ─────────────────────────────────────────────────────────────────────────────
// Slot
// ─────────────────────────────────────────────────────────────────────────────

function decodeSlotUpdateProto(proto: any): SlotUpdate {
  const info = proto.slotInfo ?? proto.slot_info;
  if (!info) throw ClientError.decode('missing required field: slot_info');
  return {
    slot: BigInt(info.slot ?? 0),
    parent: info.parent !== undefined ? BigInt(info.parent) : undefined,
    status: decodeSlotStatus(info.status),
  };
}

function decodeSlotStatus(s: number | string | undefined): SlotStatus {
  if (s === 1 || s === 'Processed') return SlotStatus.Processed;
  if (s === 2 || s === 'Rooted') return SlotStatus.Rooted;
  if (s === 3 || s === 'Confirmed') return SlotStatus.Confirmed;
  if (s === 4 || s === 'FirstShredReceived') return SlotStatus.FirstShredReceived;
  if (s === 5 || s === 'Completed') return SlotStatus.Completed;
  if (s === 6 || s === 'CreatedBank') return SlotStatus.CreatedBank;
  if (s === 7 || s === 'Dead') return SlotStatus.Dead;
  return SlotStatus.Unknown;
}

// ─────────────────────────────────────────────────────────────────────────────
// Entry
// ─────────────────────────────────────────────────────────────────────────────

export function decodeEntryInfoProto(entry: any): EntryUpdate {
  if (!entry) throw ClientError.decode('missing required field: entry');
  const hash = makeData(entry.hash);
  return {
    slot: BigInt(entry.slot ?? 0),
    index: BigInt(entry.index ?? 0),
    numHashes: BigInt(entry.numHashes ?? entry.num_hashes ?? 0),
    hash,
    executedTransactionCount: BigInt(entry.executedTransactionCount ?? entry.executed_transaction_count ?? 0),
    startingTransactionIndex: BigInt(entry.startingTransactionIndex ?? entry.starting_transaction_index ?? 0),
    hashBase58(): string { return toBase58(hash); },
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/** Safely convert proto bytes (Buffer | Uint8Array | undefined) to Uint8Array. */
function makeData(bytes: any): Uint8Array {
  if (!bytes) return new Uint8Array(0);
  if (bytes instanceof Uint8Array) return bytes;
  if (Buffer.isBuffer(bytes)) return new Uint8Array(bytes);
  return new Uint8Array(0);
}
