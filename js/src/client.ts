/**
 * Solstream SDK — gRPC streaming client
 *
 * Provides subscribe() and subscribeBlocks() with:
 *   - Automatic reconnection with exponential back-off
 *   - Optional slot-replay on reconnect
 *   - Dynamic subscription updates via StreamHandle.write()
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

// ─────────────────────────────────────────────────────────────────────────────
// Proto loading
// ─────────────────────────────────────────────────────────────────────────────

const PROTO_DIR = path.resolve(__dirname, '../proto');
const STREAMING_PROTO = path.join(PROTO_DIR, 'streaming.proto');

const LOADER_OPTIONS: protoLoader.Options = {
  keepCase: false,       // camelCase field names
  longs: String,         // uint64 → string (safe for JS)
  enums: String,         // enum → string name
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

function getServiceClient(
  endpoint: string,
  credentials: grpc.ChannelCredentials,
  channelOptions?: grpc.ChannelOptions,
): grpc.ServiceClientConstructor {
  const proto = loadProto() as any;
  const ServiceCtor: grpc.ServiceClientConstructor =
    proto.streaming.StreamingService;
  return new (ServiceCtor as any)(endpoint, credentials, channelOptions ?? {});
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
  const state: ActiveStream = { call: null, cancelled: false };

  const {
    maxReconnectAttempts = Infinity,
    baseReconnectDelayMs = 1_000,
    maxReconnectDelayMs = 30_000,
    replay = false,
  } = config;

  const { channelCreds, callMeta } = makeCredentials(config.endpoint, config.apiKey);
  const host = normalizeEndpoint(config.endpoint);

  const connect = (attempt: number): void => {
    if (state.cancelled) return;

    const client = getServiceClient(host, channelCreds, config.channelOptions);

    // Inject replay fromSlot when enabled and we have a last-known slot
    const req = buildRequest(request, replay, state.lastSlot);

    const call: grpc.ClientReadableStream<any> = callMeta
      ? (client as any).subscribe(req, callMeta)
      : (client as any).subscribe(req);
    state.call = call;

    call.on('data', async (raw: any) => {
      call.pause(); // backpressure: prevent grpc-js buffer overflow on bursts
      if (replay) {
        const slot = extractSlot(raw);
        if (slot !== undefined) state.lastSlot = slot;
      }
      try {
        await onData(raw as SubscribeUpdate);
      } catch (err) {
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
      scheduleReconnect(attempt);
    });

    call.on('end', () => {
      if (state.cancelled) return;
      scheduleReconnect(attempt);
    });
  };

  const scheduleReconnect = (prevAttempt: number): void => {
    if (state.cancelled) return;
    const nextAttempt = prevAttempt + 1;
    if (nextAttempt > maxReconnectAttempts) {
      if (onError) {
        onError(new Error(`Solstream: max reconnect attempts (${maxReconnectAttempts}) reached`));
      }
      return;
    }
    const ms = backoff(prevAttempt, baseReconnectDelayMs, maxReconnectDelayMs);
    delay(ms).then(() => connect(nextAttempt));
  };

  connect(0);

  const handle: StreamHandle = {
    id: streamId,
    cancel() {
      state.cancelled = true;
      state.call?.cancel();
    },
    write(req: SubscribeRequest): Promise<void> {
      return new Promise((resolve, reject) => {
        if (!state.call) {
          reject(new Error('Stream not active'));
          return;
        }
        (state.call as any).write(req, (err: Error | null) => {
          if (err) reject(err);
          else resolve();
        });
      });
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
  const state: ActiveStream = { call: null, cancelled: false };

  const {
    maxReconnectAttempts = Infinity,
    baseReconnectDelayMs = 1_000,
    maxReconnectDelayMs = 30_000,
    replay = false,
  } = config;

  const { channelCreds, callMeta } = makeCredentials(config.endpoint, config.apiKey);
  const host = normalizeEndpoint(config.endpoint);

  const connect = (attempt: number): void => {
    if (state.cancelled) return;

    const client = getServiceClient(host, channelCreds, config.channelOptions);

    // Inject replay fromSlot when enabled
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
      scheduleReconnect(attempt);
    });

    call.on('end', () => {
      if (!state.cancelled) scheduleReconnect(attempt);
    });
  };

  const scheduleReconnect = (prevAttempt: number): void => {
    if (state.cancelled) return;
    const nextAttempt = prevAttempt + 1;
    if (nextAttempt > maxReconnectAttempts) {
      if (onError) {
        onError(new Error(`Solstream: max reconnect attempts (${maxReconnectAttempts}) reached`));
      }
      return;
    }
    delay(backoff(prevAttempt, baseReconnectDelayMs, maxReconnectDelayMs))
      .then(() => connect(nextAttempt));
  };

  connect(0);

  const handle: StreamHandle = {
    id: streamId,
    cancel() {
      state.cancelled = true;
      state.call?.cancel();
    },
    write(_req: SubscribeRequest): Promise<void> {
      return Promise.reject(new Error('write() is not supported on block streams'));
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
