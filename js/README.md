# Solstream SDK — JavaScript / TypeScript

High-performance gRPC client for real-time Solana data streaming via **Solstream**.

- **Fluent builder API** — compose account, transaction, and slot filters in a single call
- **Async-iterator streams** — `await stream.next()` instead of callbacks
- **Decoded types** — addresses and signatures returned as base-58 strings, no manual encoding
- **Block streaming** — full blocks with transactions and account state via `client.blocks()`
- **Automatic reconnection** — exponential back-off with slot-accurate replay
- Full TypeScript types included

---

## Installation

```bash
npm install @solstream-test/solstream-sdk
```

---

## Quick Start

### Track a wallet

```typescript
import { SolstreamClient, CommitmentLevel } from '@solstream-test/solstream-sdk';

const client = SolstreamClient.connect('https://stream.example.com', 'YOUR_API_KEY');

const stream = client
  .messages()
  .accounts('wallet')
    .account('So11111111111111111111111111111111111111112')
    .build()
  .commitment(CommitmentLevel.CONFIRMED)
  .subscribe();

process.on('SIGINT', () => { stream.cancel(); process.exit(0); });

while (true) {
  const update = await stream.next();
  if (!update) break;
  const payload = update.decode();
  if (payload.kind === 'account') {
    console.log(`slot=${payload.data.slot}  lamports=${payload.data.lamports}`);
  }
}
```

### Monitor program transactions

```typescript
import { SolstreamClient, CommitmentLevel } from '@solstream-test/solstream-sdk';

const client = SolstreamClient.connect('https://stream.example.com', 'YOUR_API_KEY');

const stream = client
  .messages()
  .transactions('program-txs')
    .includeAccount('JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4')
    .excludeVotes()
    .excludeFailed()
    .build()
  .commitment(CommitmentLevel.CONFIRMED)
  .subscribe();

while (true) {
  const update = await stream.next();
  if (!update) break;
  const payload = update.decode();
  if (payload.kind === 'transaction') {
    const tx = payload.data;
    console.log(`${tx.signature}  fee=${tx.fee}  cu=${tx.computeUnitsConsumed}`);
  }
}
```

### Subscribe to full blocks

```typescript
import { SolstreamClient } from '@solstream-test/solstream-sdk';

const client = SolstreamClient.connect('https://stream.example.com', 'YOUR_API_KEY');

const stream = client
  .blocks()
  .withTransactions(true)
  .withAccounts(true)
  .subscribe();

while (true) {
  const block = await stream.next();
  if (!block) break;
  console.log(`slot=${block.block.slot}  txns=${block.transactions.length}`);
}
```

### Automatic reconnection with slot replay

```typescript
import { SolstreamClient, CommitmentLevel, SlotStatus } from '@solstream-test/solstream-sdk';
import type { ReconnectConfig } from '@solstream-test/solstream-sdk';

const client = SolstreamClient.connect('https://stream.example.com', 'YOUR_API_KEY');

const reconnect: ReconnectConfig = {
  maxAttempts: 10,       // undefined = unlimited
  initialBackoffMs: 500,
  maxBackoffMs: 30_000,
  backoffFactor: 2.0,
};

const stream = client
  .messages()
  .slots('slots')
    .filterByCommitment(true)
    .build()
  .commitment(CommitmentLevel.CONFIRMED)
  .reconnect(reconnect)
  .subscribe();

while (true) {
  const update = await stream.next();
  if (!update) break;
  const payload = update.decode();
  if (payload.kind === 'slot' && payload.data.status === SlotStatus.Confirmed) {
    console.log(`slot ${payload.data.slot}  reconnects=${stream.attempts()}`);
  }
}
```

---

## SolstreamClient

```typescript
// Connect (synchronous — no await needed)
const client = SolstreamClient.connect(endpoint, apiKey, channelOptions?);

client.messages()  // → RequestBuilder  (accounts, slots, transactions, block-metas, entries)
client.blocks()    // → BlockRequestBuilder  (full blocks)
client.close()     // close the underlying gRPC channel
```

---

## RequestBuilder (client.messages())

| Method | Description |
|---|---|
| `.accounts(name)` | Start a named account filter → `AccountFilterBuilder` |
| `.transactions(name)` | Start a named transaction filter → `TransactionFilterBuilder` |
| `.slots(name)` | Start a named slot filter → `SlotFilterBuilder` |
| `.commitment(level)` | Set minimum commitment level |
| `.fromSlot(slot)` | Replay from this slot (inclusive) |
| `.includeBlockInfos()` | Include `blockMeta` updates |
| `.includeEntries()` | Include ledger entry updates |
| `.dataSlice(offset, length)` | Trim account data to a byte range |
| `.reconnect(config)` | Enable automatic reconnection |
| `.withRawRequest(req)` | Override the entire request object |
| `.subscribe()` | Open the stream → `MessageStream` |

### AccountFilterBuilder

```typescript
client.messages()
  .accounts('my-filter')
    .account(pubkey)            // match a specific address
    .owner(program)             // match by owner program
    .dataSize(165)              // match by data length
    .tokenAccountState()        // SPL token accounts only
    .memcmp(offset, bytes)      // match data at offset
    .lamportsGt(1_000_000_000n) // > 1 SOL
    .lamportsLt(amount)
    .lamportsEq(amount)
    .lamportsNe(amount)
    .nonemptyTxnSignature()
    .build()
```

### TransactionFilterBuilder

```typescript
client.messages()
  .transactions('my-filter')
    .includeAccount(pubkey)   // program must appear in tx
    .excludeAccount(pubkey)   // program must not appear
    .requireAccount(pubkey)   // program must appear (AND)
    .excludeVotes()
    .excludeFailed()
    .signature(base58)        // exact signature match
    .build()
```

### SlotFilterBuilder

```typescript
client.messages()
  .slots('my-filter')
    .filterByCommitment(true)  // only emit at subscription commitment
    .build()
```

---

## BlockRequestBuilder (client.blocks())

```typescript
client.blocks()
  .withTransactions(true)
  .withAccounts(true)
  .withEntries(false)
  .accountFilter(pubkey)     // only blocks referencing this account
  .fromSlot(slot)
  .reconnect(config)
  .withRawRequest(req)
  .subscribe()               // → BlockStream
```

---

## Decoded types

`stream.next()` on a `MessageStream` returns a `StreamUpdate`. Call `.decode()` to get an `UpdatePayload` discriminated union:

```typescript
const update = await stream.next();   // StreamUpdate | null
const payload = update.decode();       // UpdatePayload

switch (payload.kind) {
  case 'account':     // payload.data: AccountUpdate
  case 'transaction': // payload.data: TransactionUpdate
  case 'slot':        // payload.data: SlotUpdate
  case 'blockMeta':   // payload.data: BlockInfo
  case 'entry':       // payload.data: EntryUpdate
}
```

All binary fields are pre-decoded:
- `AccountUpdate.pubkey` / `.owner` — base-58 strings
- `TransactionUpdate.signature` / `.accountKeys[]` / `.recentBlockhash` — base-58 strings
- `TransactionUpdate.success` — `boolean` (no manual error check needed)
- `BlockUpdate.block.blockhash` — base-58 string

---

## ReconnectConfig

| Field | Type | Default | Description |
|---|---|---|---|
| `maxAttempts` | `number` | `undefined` (unlimited) | Stop after N consecutive failures |
| `initialBackoffMs` | `number` | `500` | Delay before first retry |
| `maxBackoffMs` | `number` | `30000` | Cap on retry delay |
| `backoffFactor` | `number` | `2.0` | Multiplier per attempt |

---

## Stream API

`MessageStream` and `BlockStream` share the same interface:

| Method | Description |
|---|---|
| `await stream.next()` | Return next item, or `null` when stream ends |
| `stream.cancel()` | Cancel the stream; rejects any pending `.next()` |
| `stream.lastSlot()` | Slot of the last successfully received message |
| `stream.attempts()` | Consecutive reconnect attempts since last success |

---

## Advanced: multiple named filters

The server evaluates each named filter independently and tags each update with the filter name(s) it matched. Access them via `update.filters`:

```typescript
const update = await stream.next();
console.log(update.filters);  // e.g. ['spl-token-accounts']
const payload = update.decode();
```

---

## Examples

```bash
cd js
SOLSTREAM_ENDPOINT=https://... SOLSTREAM_API_KEY=... npx ts-node examples/subscribe-accounts.ts
SOLSTREAM_ENDPOINT=https://... SOLSTREAM_API_KEY=... npx ts-node examples/subscribe-transactions.ts
SOLSTREAM_ENDPOINT=https://... SOLSTREAM_API_KEY=... npx ts-node examples/subscribe-blocks.ts
SOLSTREAM_ENDPOINT=https://... SOLSTREAM_API_KEY=... npx ts-node examples/subscribe-slots.ts
SOLSTREAM_ENDPOINT=https://... SOLSTREAM_API_KEY=... npx ts-node examples/subscribe-filtered-accounts.ts
```

Or via npm scripts:

```bash
npm run example:accounts
npm run example:transactions
npm run example:blocks
npm run example:slots
```

---

## Legacy callback API

The original callback-based API is still available for backward compatibility:

```typescript
import { subscribe, subscribeBlocks } from '@solstream-test/solstream-sdk';

const handle = await subscribe(config, request, onData, onError);
handle.cancel();
```

---

## Building from source

```bash
cd js
npm install
npm run build
```
