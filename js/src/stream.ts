/**
 * MessageStream and BlockStream — async-iterator style streams.
 *
 * Mirrors the Rust SDK's stream.rs: MessageStream::next() /
 * BlockStream::next(), last_slot(), attempts().
 */

import * as grpc from '@grpc/grpc-js';
import { ClientError } from './error';
import { wrapRawUpdate, decodeBlockUpdateProto } from './decode';
import type { BlockUpdate, ReconnectConfig, StreamUpdate } from './decoded-types';
import type { SubscribeBlockRequest, SubscribeRequest } from './types';

// ─────────────────────────────────────────────────────────────────────────────
// Shared internals
// ─────────────────────────────────────────────────────────────────────────────

type Waiter<T> = { resolve: (v: T | null) => void; reject: (e: Error) => void };

// Max items to buffer before pausing the underlying gRPC call.
const HIGH_WATER = 100;

abstract class BaseStream<T> {
  protected call: grpc.ClientReadableStream<any> | null = null;
  protected currentRequest: any;

  private queue: Array<T | null> = [];
  private waiters: Array<Waiter<T>> = [];
  private pendingError: Error | null = null;
  private paused = false;
  protected done = false;

  protected lastSlotValue: bigint | undefined;
  protected attemptCount = 0;
  protected currentBackoffMs: number;

  constructor(
    protected readonly grpcClient: grpc.Client,
    request: any,
    protected readonly callMeta: grpc.Metadata | undefined,
    protected readonly reconnectConfig: ReconnectConfig | undefined,
  ) {
    this.currentRequest = request;
    this.currentBackoffMs = reconnectConfig?.initialBackoffMs ?? 500;
    this.attach();
  }

  // ── abstract / overridable ──────────────────────────────────────────────

  protected abstract makeCall(req: any): grpc.ClientReadableStream<any>;
  protected abstract decodeRaw(raw: any): T;
  protected abstract extractSlot(item: T): bigint | undefined;

  // ── stream lifecycle ────────────────────────────────────────────────────

  protected attach(): void {
    if (this.done) return;

    // Inject replay slot when reconnecting
    if (this.lastSlotValue !== undefined && this.reconnectConfig) {
      this.currentRequest = { ...this.currentRequest, fromSlot: this.lastSlotValue };
    }

    const call = this.makeCall(this.currentRequest);
    this.call = call;

    call.on('data', (raw: any) => {
      call.pause(); // prevent grpc-js internal 4 MB batch-buffer overflow
      let item: T;
      try {
        item = this.decodeRaw(raw);
      } catch (e) {
        this.deliverError(e as Error);
        call.resume();
        return;
      }
      const slot = this.extractSlot(item);
      if (slot !== undefined) this.lastSlotValue = slot;
      this.attemptCount = 0;
      this.currentBackoffMs = this.reconnectConfig?.initialBackoffMs ?? 500;
      this.deliver(item);
      call.resume();
    });

    call.on('error', (err: Error) => {
      if (this.done) return;
      this.scheduleReconnect(err);
    });

    call.on('end', () => {
      if (this.done) return;
      if (!this.reconnectConfig) {
        // No reconnect policy: clean end
        this.deliver(null);
        this.done = true;
      } else {
        this.scheduleReconnect(new Error('stream ended by server'));
      }
    });
  }

  private deliver(item: T | null): void {
    if (this.waiters.length > 0) {
      this.waiters.shift()!.resolve(item);
    } else {
      this.queue.push(item);
      if (this.queue.length >= HIGH_WATER && !this.paused) {
        this.paused = true;
        this.call?.pause();
      }
    }
  }

  private deliverError(err: Error): void {
    if (this.waiters.length > 0) {
      this.waiters.shift()!.reject(err);
    } else {
      this.pendingError = err;
    }
  }

  private scheduleReconnect(err: Error): void {
    const cfg = this.reconnectConfig;
    if (!cfg) {
      this.deliverError(ClientError.transport(err));
      this.done = true;
      return;
    }

    const max = cfg.maxAttempts;
    if (max !== undefined && this.attemptCount >= max) {
      this.deliverError(
        new ClientError('status', `max reconnect attempts (${max}) reached`, err),
      );
      this.done = true;
      return;
    }

    this.attemptCount++;
    const backoffMs = this.currentBackoffMs;
    this.currentBackoffMs = Math.min(
      this.currentBackoffMs * (cfg.backoffFactor ?? 2.0),
      cfg.maxBackoffMs ?? 30_000,
    );

    setTimeout(() => this.attach(), backoffMs);
  }

  // ── public API ──────────────────────────────────────────────────────────

  /**
   * Wait for and return the next item.
   * Returns `null` when the server closes the stream cleanly (no reconnect
   * policy) or throws a `ClientError` after exhausting reconnect attempts.
   */
  async next(): Promise<T | null> {
    if (this.pendingError) {
      const err = this.pendingError;
      this.pendingError = null;
      throw err;
    }
    if (this.queue.length > 0) {
      const item = this.queue.shift()!;
      if (this.paused && this.queue.length < HIGH_WATER / 2) {
        this.paused = false;
        this.call?.resume();
      }
      return item;
    }
    if (this.done) return null;
    return new Promise<T | null>((resolve, reject) => {
      this.waiters.push({ resolve, reject });
    });
  }

  /** Slot number of the last successfully received message. */
  lastSlot(): bigint | undefined { return this.lastSlotValue; }

  /** Number of consecutive reconnect attempts since the last successful message. */
  attempts(): number { return this.attemptCount; }

  /** Cancel the stream and reject any pending `.next()` calls. */
  cancel(): void {
    this.done = true;
    this.call?.cancel();
    const err = ClientError.cancelled();
    for (const w of this.waiters) w.reject(err);
    this.waiters = [];
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// MessageStream
// ─────────────────────────────────────────────────────────────────────────────

/**
 * A message stream from the `Subscribe` RPC.
 *
 * Obtained from `RequestBuilder.subscribe()`.
 *
 * @example
 * ```ts
 * const client = await SolstreamClient.connect(endpoint, apiKey);
 * const stream = await client.messages()
 *   .slots('all')
 *     .build()
 *   .commitment(CommitmentLevel.CONFIRMED)
 *   .subscribe();
 *
 * while (true) {
 *   const update = await stream.next();
 *   if (!update) break;               // server closed stream
 *   const payload = update.decode();  // base-58 decoded
 *   if (payload.kind === 'slot') console.log(payload.data.slot);
 * }
 * ```
 */
export class MessageStream extends BaseStream<StreamUpdate> {
  constructor(
    grpcClient: grpc.Client,
    request: SubscribeRequest,
    callMeta: grpc.Metadata | undefined,
    reconnectConfig: ReconnectConfig | undefined,
  ) {
    super(grpcClient, request, callMeta, reconnectConfig);
  }

  protected makeCall(req: any): grpc.ClientReadableStream<any> {
    return this.callMeta
      ? (this.grpcClient as any).subscribe(req, this.callMeta)
      : (this.grpcClient as any).subscribe(req);
  }

  protected decodeRaw(raw: any): StreamUpdate {
    return wrapRawUpdate(raw);
  }

  protected extractSlot(item: StreamUpdate): bigint | undefined {
    const s = item.slot();
    return s !== BigInt(0) ? s : undefined;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// BlockStream
// ─────────────────────────────────────────────────────────────────────────────

/**
 * A block stream from the `SubscribeBlocks` RPC.
 *
 * Obtained from `BlockRequestBuilder.subscribe()`.
 *
 * @example
 * ```ts
 * const stream = await client.blocks()
 *   .withTransactions(true)
 *   .subscribe();
 *
 * while (true) {
 *   const block = await stream.next();
 *   if (!block) break;
 *   console.log(`block ${block.block.slot} — ${block.transactions.length} txns`);
 * }
 * ```
 */
export class BlockStream extends BaseStream<BlockUpdate> {
  constructor(
    grpcClient: grpc.Client,
    request: SubscribeBlockRequest,
    callMeta: grpc.Metadata | undefined,
    reconnectConfig: ReconnectConfig | undefined,
  ) {
    super(grpcClient, request, callMeta, reconnectConfig);
  }

  protected makeCall(req: any): grpc.ClientReadableStream<any> {
    return this.callMeta
      ? (this.grpcClient as any).subscribeBlocks(req, this.callMeta)
      : (this.grpcClient as any).subscribeBlocks(req);
  }

  protected decodeRaw(raw: any): BlockUpdate {
    return decodeBlockUpdateProto(raw);
  }

  protected extractSlot(item: BlockUpdate): bigint | undefined {
    return item.block.slot !== BigInt(0) ? item.block.slot : undefined;
  }
}
