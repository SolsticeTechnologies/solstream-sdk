/**
 * Fluent request builders — mirrors the Rust SDK's builder.rs.
 *
 * Usage:
 *   client.messages()
 *     .accounts('token-accs')
 *       .owner(TOKEN_PROGRAM)
 *       .tokenAccountState()
 *       .build()
 *     .transactions('token-txns')
 *       .includeAccount(TOKEN_PROGRAM)
 *       .excludeVotes()
 *       .excludeFailed()
 *       .build()
 *     .commitment(CommitmentLevel.CONFIRMED)
 *     .subscribe()   // returns MessageStream
 */

import * as grpc from '@grpc/grpc-js';
import { CommitmentLevel } from './types';
import type {
  SubscribeBlockRequest,
  SubscribeRequest,
  SubscribeRequestFilterAccounts,
  SubscribeRequestFilterSlots,
  SubscribeRequestFilterTransactions,
} from './types';
import type { ReconnectConfig } from './decoded-types';
import { MessageStream, BlockStream } from './stream';

// ─────────────────────────────────────────────────────────────────────────────
// RequestBuilder
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Fluent builder for a `Subscribe` RPC request.
 *
 * Call `subscribe()` when the filters are ready to open the stream.
 */
export class RequestBuilder {
  private req: SubscribeRequest = {};
  private reconnectCfg: ReconnectConfig | undefined;

  constructor(
    private readonly grpcClient: grpc.Client,
    private readonly callMeta: grpc.Metadata | undefined,
  ) {}

  /** Set the minimum commitment level. */
  commitment(level: CommitmentLevel): this {
    this.req.commitment = level;
    return this;
  }

  /** Replay from this slot (inclusive), then continue live. */
  fromSlot(slot: bigint | number): this {
    this.req.fromSlot = BigInt(slot);
    return this;
  }

  /** Arbitrary client identifier sent to the server (useful for debugging). */
  clientId(id: string): this {
    this.req.clientId = id;
    return this;
  }

  /** Include `BlockMeta` updates in the stream. */
  includeBlockInfos(include = true): this {
    this.req.includeBlockInfos = include;
    return this;
  }

  /** Include `Entry` updates in the stream. */
  includeEntries(include = true): this {
    this.req.includeEntries = include;
    return this;
  }

  /**
   * Only return the account data bytes in the given range
   * `[offset, offset+length)`.  Multiple slices are concatenated by the server.
   */
  dataSlice(offset: bigint | number, length: bigint | number): this {
    this.req.accountsDataSlice ??= [];
    this.req.accountsDataSlice.push({ offset: BigInt(offset), length: BigInt(length) });
    return this;
  }

  /** Enable automatic reconnection with the given policy. */
  reconnect(config: ReconnectConfig): this {
    this.reconnectCfg = config;
    return this;
  }

  /**
   * Override the entire request with a hand-crafted object.
   * Useful when the builder API is insufficient.
   */
  withRawRequest(request: SubscribeRequest): this {
    this.req = request;
    return this;
  }

  /** Start a named account filter. Call `.build()` to return here. */
  accounts(filterName: string): AccountFilterBuilder {
    return new AccountFilterBuilder(this, filterName);
  }

  /** Start a named transaction filter. Call `.build()` to return here. */
  transactions(filterName: string): TransactionFilterBuilder {
    return new TransactionFilterBuilder(this, filterName);
  }

  /** Start a named slot filter. Call `.build()` to return here. */
  slots(filterName: string): SlotFilterBuilder {
    return new SlotFilterBuilder(this, filterName);
  }

  /** Open the subscription stream. */
  subscribe(): MessageStream {
    return new MessageStream(this.grpcClient, this.req, this.callMeta, this.reconnectCfg);
  }

  // Internal: used by sub-builders to insert filters
  _setAccountFilter(name: string, filter: SubscribeRequestFilterAccounts): void {
    this.req.accounts ??= {};
    this.req.accounts[name] = filter;
  }
  _setTransactionFilter(name: string, filter: SubscribeRequestFilterTransactions): void {
    this.req.transactions ??= {};
    this.req.transactions[name] = filter;
  }
  _setSlotFilter(name: string, filter: SubscribeRequestFilterSlots): void {
    this.req.slots ??= {};
    this.req.slots[name] = filter;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// AccountFilterBuilder
// ─────────────────────────────────────────────────────────────────────────────

export class AccountFilterBuilder {
  private filter: SubscribeRequestFilterAccounts = {};

  constructor(
    private readonly parent: RequestBuilder,
    private readonly name: string,
  ) {}

  /** Match a specific account address (base-58). */
  account(pubkey: string): this {
    this.filter.account ??= [];
    this.filter.account.push(pubkey);
    return this;
  }

  /** Match any of these account addresses. */
  accountList(pubkeys: string[]): this {
    this.filter.account ??= [];
    this.filter.account.push(...pubkeys);
    return this;
  }

  /** Match accounts owned by this program (base-58). */
  owner(program: string): this {
    this.filter.owner ??= [];
    this.filter.owner.push(program);
    return this;
  }

  /** Match accounts owned by any of these programs. */
  ownerList(programs: string[]): this {
    this.filter.owner ??= [];
    this.filter.owner.push(...programs);
    return this;
  }

  /** Require a non-empty transaction signature on the last update. */
  nonemptyTxnSignature(): this {
    this.filter.nonemptyTxnSignature = true;
    return this;
  }

  /** Match accounts whose data is exactly `size` bytes. */
  dataSize(size: bigint | number): this {
    this.filter.filters ??= [];
    this.filter.filters.push({ datasize: BigInt(size) });
    return this;
  }

  /** Match accounts where `data[offset..offset+pattern.length] == pattern`. */
  memcmp(offset: bigint | number, pattern: Uint8Array): this {
    this.filter.filters ??= [];
    this.filter.filters.push({ memcmp: { offset: BigInt(offset), bytes: pattern } });
    return this;
  }

  /** Match accounts where the data at `offset` equals `base58` when decoded. */
  memcmpBase58(offset: bigint | number, base58: string): this {
    this.filter.filters ??= [];
    this.filter.filters.push({ memcmp: { offset: BigInt(offset), base58 } });
    return this;
  }

  /** Match only token accounts (SPL Token program internal state). */
  tokenAccountState(): this {
    this.filter.filters ??= [];
    this.filter.filters.push({ tokenAccountState: true });
    return this;
  }

  /** Match accounts with exactly `lamports` lamports. */
  lamportsEq(lamports: bigint | number): this {
    this.filter.filters ??= [];
    this.filter.filters.push({ lamports: { eq: BigInt(lamports) } });
    return this;
  }

  /** Match accounts with any lamport balance other than `lamports`. */
  lamportsNe(lamports: bigint | number): this {
    this.filter.filters ??= [];
    this.filter.filters.push({ lamports: { ne: BigInt(lamports) } });
    return this;
  }

  /** Match accounts with fewer than `lamports` lamports. */
  lamportsLt(lamports: bigint | number): this {
    this.filter.filters ??= [];
    this.filter.filters.push({ lamports: { lt: BigInt(lamports) } });
    return this;
  }

  /** Match accounts with more than `lamports` lamports. */
  lamportsGt(lamports: bigint | number): this {
    this.filter.filters ??= [];
    this.filter.filters.push({ lamports: { gt: BigInt(lamports) } });
    return this;
  }

  /** Finish this filter and return to the parent `RequestBuilder`. */
  build(): RequestBuilder {
    this.parent._setAccountFilter(this.name, this.filter);
    return this.parent;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// TransactionFilterBuilder
// ─────────────────────────────────────────────────────────────────────────────

export class TransactionFilterBuilder {
  private filter: SubscribeRequestFilterTransactions = {};

  constructor(
    private readonly parent: RequestBuilder,
    private readonly name: string,
  ) {}

  /** Include transactions that reference this account. */
  includeAccount(pubkey: string): this {
    this.filter.accountInclude ??= [];
    this.filter.accountInclude.push(pubkey);
    return this;
  }

  /** Include transactions that reference any of these accounts. */
  includeAccounts(pubkeys: string[]): this {
    this.filter.accountInclude ??= [];
    this.filter.accountInclude.push(...pubkeys);
    return this;
  }

  /** Exclude transactions that reference this account. */
  excludeAccount(pubkey: string): this {
    this.filter.accountExclude ??= [];
    this.filter.accountExclude.push(pubkey);
    return this;
  }

  /** Exclude transactions that reference any of these accounts. */
  excludeAccounts(pubkeys: string[]): this {
    this.filter.accountExclude ??= [];
    this.filter.accountExclude.push(...pubkeys);
    return this;
  }

  /** Require all of these accounts to appear in a matching transaction. */
  requireAccount(pubkey: string): this {
    this.filter.accountRequired ??= [];
    this.filter.accountRequired.push(pubkey);
    return this;
  }

  /** Require all of these accounts to appear in a matching transaction. */
  requireAccounts(pubkeys: string[]): this {
    this.filter.accountRequired ??= [];
    this.filter.accountRequired.push(...pubkeys);
    return this;
  }

  /** Filter by vote status. `true` = only votes; `false` = no votes. */
  vote(include: boolean): this {
    this.filter.vote = include;
    return this;
  }

  /** Exclude vote transactions. */
  excludeVotes(): this { return this.vote(false); }

  /** Filter by failure status. `true` = only failed; `false` = only successful. */
  failed(include: boolean): this {
    this.filter.failed = include;
    return this;
  }

  /** Exclude failed transactions. */
  excludeFailed(): this { return this.failed(false); }

  /** Match only this exact transaction signature (base-58). */
  signature(sig: string): this {
    this.filter.signature = sig;
    return this;
  }

  /** Finish this filter and return to the parent `RequestBuilder`. */
  build(): RequestBuilder {
    this.parent._setTransactionFilter(this.name, this.filter);
    return this.parent;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// SlotFilterBuilder
// ─────────────────────────────────────────────────────────────────────────────

export class SlotFilterBuilder {
  private filter: SubscribeRequestFilterSlots = {};

  constructor(
    private readonly parent: RequestBuilder,
    private readonly name: string,
  ) {}

  /** Only emit slot updates that match the subscription's commitment level. */
  filterByCommitment(enable = true): this {
    this.filter.filterByCommitment = enable;
    return this;
  }

  /** Finish this filter and return to the parent `RequestBuilder`. */
  build(): RequestBuilder {
    this.parent._setSlotFilter(this.name, this.filter);
    return this.parent;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// BlockRequestBuilder
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Fluent builder for a `SubscribeBlocks` RPC request.
 *
 * Call `subscribe()` to open the stream.
 */
export class BlockRequestBuilder {
  private req: SubscribeBlockRequest = {};
  private reconnectCfg: ReconnectConfig | undefined;

  constructor(
    private readonly grpcClient: grpc.Client,
    private readonly callMeta: grpc.Metadata | undefined,
  ) {}

  /** Arbitrary client identifier. */
  clientId(id: string): this {
    this.req.clientId = id;
    return this;
  }

  /** Replay from this slot (inclusive). */
  fromSlot(slot: bigint | number): this {
    this.req.fromSlot = BigInt(slot);
    return this;
  }

  /** Only include blocks that reference this account. */
  accountFilter(pubkey: string): this {
    this.req.accountInclude ??= [];
    this.req.accountInclude.push(pubkey);
    return this;
  }

  /** Only include blocks that reference any of these accounts. */
  accountFilters(pubkeys: string[]): this {
    this.req.accountInclude ??= [];
    this.req.accountInclude.push(...pubkeys);
    return this;
  }

  /** Include transaction data in each `BlockUpdate`. */
  withTransactions(include = true): this {
    this.req.includeTransactions = include;
    return this;
  }

  /** Include account-state data in each `BlockUpdate`. */
  withAccounts(include = true): this {
    this.req.includeAccounts = include;
    return this;
  }

  /** Include entry data in each `BlockUpdate`. */
  withEntries(include = true): this {
    this.req.includeEntries = include;
    return this;
  }

  /** Enable automatic reconnection. */
  reconnect(config: ReconnectConfig): this {
    this.reconnectCfg = config;
    return this;
  }

  /** Override the entire request. */
  withRawRequest(request: SubscribeBlockRequest): this {
    this.req = request;
    return this;
  }

  /** Open the block subscription stream. */
  subscribe(): BlockStream {
    return new BlockStream(this.grpcClient, this.req, this.callMeta, this.reconnectCfg);
  }
}
