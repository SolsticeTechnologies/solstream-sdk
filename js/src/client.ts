/**
 * Solstream SDK — gRPC streaming client
 *
 * Primary API  : SolstreamClient class (stateful, single channel, builders)
 * Legacy API   : subscribe() / subscribeBlocks() callback functions
 */

import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import * as path from 'path';
import { randomUUID } from 'crypto';

import type {
  SolstreamConfig,
  SubscribeRequest,
  SubscribeBlockRequest,
  SubscribeUpdate,
  SubscribeBlockUpdate,
  StreamHandle,
} from './types';
import { RequestBuilder, BlockRequestBuilder } from './builder';

// ─────────────────────────────────────────────────────────────────────────────
// Proto loading
// ─────────────────────────────────────────────────────────────────────────────

const PROTO_DIR = path.resolve(__dirname, '../proto');
const STREAMING_PROTO = path.join(PROTO_DIR, 'streaming.proto');

const LOADER_OPTIONS: protoLoader.Options = {
  keepCase: false,       // camelCase field names
  longs: BigInt,         // uint64 → bigint (matches TypeScript types)
  enums: Number,         // enum → numeric value (matches TypeScript enums)
  defaults: true,
  oneofs: true,
  includeDirs: [
    PROTO_DIR,
    // google/protobuf well-known types shipped by grpc-tools / protobufjs
    path.join(require.resolve('google-proto-files'), '..'),
  ],
};

let _packageDef: protoLoader.PackageDefinition | null = null;
let _grpcObj: grpc.GrpcObject | null = null;

function loadProto(): grpc.GrpcObject {
  if (_grpcObj) return _grpcObj;
  _packageDef = protoLoader.loadSync(STREAMING_PROTO, LOADER_OPTIONS);
  _grpcObj = grpc.loadPackageDefinition(_packageDef);
  return _grpcObj;
}

// ─────────────────────────────────────────────────────────────────────────────
// Default channel options
// ─────────────────────────────────────────────────────────────────────────────

// 128 MiB — matches the Rust SDK MAX_DECODING_BYTES constant.
const MAX_MESSAGE_BYTES = 128 * 1024 * 1024;

const DEFAULT_CHANNEL_OPTIONS: grpc.ChannelOptions = {
  'grpc.max_receive_message_length': MAX_MESSAGE_BYTES,
  'grpc.max_send_message_length': -1,
  // Keep-alive pings so silent TCP drops are detected.
  'grpc.keepalive_time_ms': 30_000,
  'grpc.keepalive_timeout_ms': 5_000,
  'grpc.keepalive_permit_without_calls': 1,
  // Prefer gzip on the receive path (server must also support it).
  'grpc.default_compression_algorithm': grpc.compressionAlgorithms.gzip,
};

function getServiceClient(
  endpoint: string,
  credentials: grpc.ChannelCredentials,
  channelOptions?: grpc.ChannelOptions,
): grpc.Client {
  const proto = loadProto() as any;
  const ServiceCtor: grpc.ServiceClientConstructor =
    proto.streaming.StreamingService;
  const merged = { ...DEFAULT_CHANNEL_OPTIONS, ...channelOptions };
  return new (ServiceCtor as any)(endpoint, credentials, merged) as grpc.Client;
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

function makeCredentials(
  endpoint: string,
  apiKey?: string,
): { channelCreds: grpc.ChannelCredentials; callMeta: grpc.Metadata | undefined } {
  const isSecure =
    endpoint.startsWith('https://') || endpoint.startsWith('grpcs://');

  if (isSecure) {
    const base = grpc.credentials.createSsl();
    if (!apiKey) return { channelCreds: base, callMeta: undefined };
    const callCreds = grpc.credentials.createFromMetadataGenerator(
      (_params, callback) => {
        const meta = new grpc.Metadata();
        meta.add('x-api-key', apiKey);
        callback(null, meta);
      },
    );
    return {
      channelCreds: grpc.credentials.combineChannelCredentials(base, callCreds),
      callMeta: undefined,
    };
  }

  // Insecure: can't combine credentials, pass API key as per-call metadata instead
  const callMeta = apiKey ? new grpc.Metadata() : undefined;
  if (apiKey && callMeta) callMeta.add('x-api-key', apiKey);
  return { channelCreds: grpc.credentials.createInsecure(), callMeta };
}

function normalizeEndpoint(endpoint: string): string {
  return endpoint
    .replace(/^https?:\/\//, '')
    .replace(/^grpcs?:\/\//, '')
    .replace(/\/$/, '');
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function backoff(
  attempt: number,
  base: number,
  max: number,
): number {
  const jitter = Math.random() * 0.3 + 0.85; // 0.85–1.15
  return Math.min(base * 2 ** attempt * jitter, max);
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal stream state
// ─────────────────────────────────────────────────────────────────────────────

interface ActiveStream {
  call: grpc.ClientReadableStream<any> | null;
  client: grpc.Client | null;
  cancelled: boolean;
  lastSlot?: bigint;
}

// ─────────────────────────────────────────────────────────────────────────────
// subscribe()
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Open a streaming subscription for accounts, slots, transactions, and/or
 * block metadata.
 *
 * @example
 * ```ts
 * const stream = await subscribe(
 *   { endpoint: 'https://stream.example.com', apiKey: 'YOUR_KEY' },
 *   { accounts: { all: { account: [], owner: [], filters: [] } } },
 *   (update) => console.log(update),
 * );
 * // later …
 * stream.cancel();
 * ```
 */
export async function subscribe(
  config: SolstreamConfig,
  request: SubscribeRequest,
  onData: (update: SubscribeUpdate) => void | Promise<void>,
  onError?: (error: Error) => void | Promise<void>,
): Promise<StreamHandle> {
  const streamId = randomUUID();
  const state: ActiveStream = { call: null, client: null, cancelled: false };

  const {
    maxReconnectAttempts = Infinity,
    baseReconnectDelayMs = 1_000,
    maxReconnectDelayMs = 30_000,
    replay = false,
  } = config;

  const { channelCreds, callMeta } = makeCredentials(config.endpoint, config.apiKey);
  const host = normalizeEndpoint(config.endpoint);

  // Mutable counter reset to 0 on every successful message (mirrors Rust SDK).
  let currentAttempt = 0;

  const connect = (): void => {
    if (state.cancelled) return;

    // Close the previous channel before opening a new one to avoid leaking connections.
    state.client?.close();

    const client = getServiceClient(host, channelCreds, config.channelOptions);
    state.client = client;

    const req = buildRequest(request, replay, state.lastSlot);

    const call: grpc.ClientReadableStream<any> = callMeta
      ? (client as any).subscribe(req, callMeta)
      : (client as any).subscribe(req);
    state.call = call;

    call.on('data', async (raw: any) => {
      call.pause(); // backpressure: prevent grpc-js buffer overflow on bursts
      currentAttempt = 0; // reset on successful message
      if (replay) {
        const slot = extractSlot(raw);
        if (slot !== undefined) state.lastSlot = slot;
      }
      try {
        await onData(raw as SubscribeUpdate);
      } catch {
        /* swallow user handler errors so they don't kill the stream */
      }
      call.resume();
    });

    call.on('error', async (err: Error) => {
      if (state.cancelled) return;
      if (onError) {
        try {
          await onError(err);
        } catch {
          /* ignore */
        }
      }
      scheduleReconnect();
    });

    call.on('end', () => {
      if (!state.cancelled) scheduleReconnect();
    });
  };

  const scheduleReconnect = (): void => {
    if (state.cancelled) return;
    currentAttempt++;
    if (currentAttempt > maxReconnectAttempts) {
      onError?.(new Error(`Solstream: max reconnect attempts (${maxReconnectAttempts}) reached`));
      return;
    }
    const ms = backoff(currentAttempt - 1, baseReconnectDelayMs, maxReconnectDelayMs);
    delay(ms).then(() => connect());
  };

  connect();

  const handle: StreamHandle = {
    id: streamId,
    cancel() {
      state.cancelled = true;
      state.call?.cancel();
      state.client?.close();
    },
  };

  return handle;
}

// ─────────────────────────────────────────────────────────────────────────────
// subscribeBlocks()
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Open a block-level streaming subscription.
 *
 * @example
 * ```ts
 * const stream = await subscribeBlocks(
 *   { endpoint: 'https://stream.example.com', apiKey: 'YOUR_KEY' },
 *   { includeTransactions: true, includeAccounts: false },
 *   (blockUpdate) => console.log(blockUpdate),
 * );
 * ```
 */
export async function subscribeBlocks(
  config: SolstreamConfig,
  request: SubscribeBlockRequest,
  onData: (update: SubscribeBlockUpdate) => void | Promise<void>,
  onError?: (error: Error) => void | Promise<void>,
): Promise<StreamHandle> {
  const streamId = randomUUID();
  const state: ActiveStream = { call: null, client: null, cancelled: false };

  const {
    maxReconnectAttempts = Infinity,
    baseReconnectDelayMs = 1_000,
    maxReconnectDelayMs = 30_000,
    replay = false,
  } = config;

  const { channelCreds, callMeta } = makeCredentials(config.endpoint, config.apiKey);
  const host = normalizeEndpoint(config.endpoint);

  let currentAttempt = 0;

  const connect = (): void => {
    if (state.cancelled) return;

    state.client?.close();

    const client = getServiceClient(host, channelCreds, config.channelOptions);
    state.client = client;

    const req: SubscribeBlockRequest =
      replay && state.lastSlot !== undefined
        ? { ...request, fromSlot: state.lastSlot }
        : request;

    const call: grpc.ClientReadableStream<any> = callMeta
      ? (client as any).subscribeBlocks(req, callMeta)
      : (client as any).subscribeBlocks(req);
    state.call = call;

    call.on('data', async (raw: any) => {
      call.pause();
      currentAttempt = 0;
      if (replay && raw?.block?.slot !== undefined) {
        state.lastSlot = BigInt(raw.block.slot);
      }
      try {
        await onData(raw as SubscribeBlockUpdate);
      } catch {
        /* swallow */
      }
      call.resume();
    });

    call.on('error', async (err: Error) => {
      if (state.cancelled) return;
      if (onError) {
        try { await onError(err); } catch { /* ignore */ }
      }
      scheduleReconnect();
    });

    call.on('end', () => {
      if (!state.cancelled) scheduleReconnect();
    });
  };

  const scheduleReconnect = (): void => {
    if (state.cancelled) return;
    currentAttempt++;
    if (currentAttempt > maxReconnectAttempts) {
      onError?.(new Error(`Solstream: max reconnect attempts (${maxReconnectAttempts}) reached`));
      return;
    }
    delay(backoff(currentAttempt - 1, baseReconnectDelayMs, maxReconnectDelayMs))
      .then(() => connect());
  };

  connect();

  const handle: StreamHandle = {
    id: streamId,
    cancel() {
      state.cancelled = true;
      state.call?.cancel();
      state.client?.close();
    },
  };

  return handle;
}

// ─────────────────────────────────────────────────────────────────────────────
// Internals
// ─────────────────────────────────────────────────────────────────────────────

function buildRequest(
  req: SubscribeRequest,
  replay: boolean,
  lastSlot?: bigint,
): SubscribeRequest {
  if (!replay || lastSlot === undefined) return req;
  return { ...req, fromSlot: lastSlot };
}

/** Extract the most relevant slot number from any SubscribeUpdate shape */
function extractSlot(update: any): bigint | undefined {
  if (update?.transaction?.slot !== undefined)
    return BigInt(update.transaction.slot);
  if (update?.account?.slot !== undefined)
    return BigInt(update.account.slot);
  if (update?.slot?.slotInfo?.slot !== undefined)
    return BigInt(update.slot.slotInfo.slot);
  if (update?.blockMeta?.blockInfo?.slot !== undefined)
    return BigInt(update.blockMeta.blockInfo.slot);
  return undefined;
}

// ─────────────────────────────────────────────────────────────────────────────
// SolstreamClient — stateful client (primary API)
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Stateful gRPC client.  One instance owns one channel; all streams opened
 * from it reuse that channel rather than creating a new TCP connection.
 *
 * @example
 * ```ts
 * import { SolstreamClient, CommitmentLevel } from '@solstice/solstream-sdk';
 *
 * const client = await SolstreamClient.connect('https://stream.example.com', 'YOUR_KEY');
 *
 * const stream = client
 *   .messages()
 *   .slots('all')
 *     .build()
 *   .commitment(CommitmentLevel.CONFIRMED)
 *   .subscribe();
 *
 * while (true) {
 *   const update = await stream.next();
 *   if (!update) break;
 *   const payload = update.decode();
 *   if (payload.kind === 'slot') console.log(payload.data.slot);
 * }
 * ```
 */
export class SolstreamClient {
  private constructor(
    private readonly grpcClient: grpc.Client,
    private readonly callMeta: grpc.Metadata | undefined,
  ) {}

  /**
   * Connect to a SolStream server.
   *
   * @param endpoint - gRPC server address, e.g. `"https://stream.example.com"`
   * @param apiKey   - ASCII API key
   * @param channelOptions - Optional channel option overrides (merged over SDK defaults)
   */
  static connect(
    endpoint: string,
    apiKey: string,
    channelOptions?: grpc.ChannelOptions,
  ): SolstreamClient {
    const { channelCreds, callMeta } = makeCredentials(endpoint, apiKey);
    const host = normalizeEndpoint(endpoint);
    const client = getServiceClient(host, channelCreds, channelOptions);
    return new SolstreamClient(client, callMeta);
  }

  /** Begin building a `Subscribe` request (accounts, slots, transactions, block-metas, entries). */
  messages(): RequestBuilder {
    return new RequestBuilder(this.grpcClient, this.callMeta);
  }

  /** Begin building a `SubscribeBlocks` request for full block data. */
  blocks(): BlockRequestBuilder {
    return new BlockRequestBuilder(this.grpcClient, this.callMeta);
  }

  /** Close the underlying gRPC channel. */
  close(): void {
    this.grpcClient.close();
  }
}
