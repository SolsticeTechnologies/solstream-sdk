# Solstream SDK — JavaScript / TypeScript

High-performance gRPC client for real-time Solana data streaming via **Solstream**.

- **Subscribe** to accounts, slots, transactions, and block metadata
- **Block streaming** with optional account and transaction data
- **Automatic reconnection** with exponential back-off
- **Slot replay** — resume from the last processed slot on reconnect
- **Dynamic subscriptions** — update filters on a live stream via `StreamHandle.write()`
- Full TypeScript types included

---

## Installation

```bash
npm install @solstice/solstream-sdk
# or
yarn add @solstice/solstream-sdk
```

---

## Quick Start

### Subscribe to accounts

```typescript
import { subscribe, CommitmentLevel } from '@solstice/solstream-sdk';

const stream = await subscribe(
  {
    endpoint: 'https://stream.example.com',
    apiKey: 'YOUR_API_KEY',
    replay: true, // resume from last slot on reconnect
  },
  {
    accounts: {
      'my-filter': {
        account: ['TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'],
        owner: [],
        filters: [],
      },
    },
    commitment: CommitmentLevel.CONFIRMED,
  },
  (update) => {
    if (update.account) {
      console.log('Account update:', update.account);
    }
  },
  (error) => console.error('Stream error:', error),
);

process.on('SIGINT', () => {
  stream.cancel();
  process.exit(0);
});
```

### Subscribe to transactions

```typescript
import { subscribe, CommitmentLevel } from '@solstice/solstream-sdk';

const stream = await subscribe(
  { endpoint: 'https://stream.example.com', apiKey: 'YOUR_API_KEY' },
  {
    transactions: {
      'non-vote': {
        vote: false,
        failed: false,
        accountInclude: [],
        accountExclude: [],
        accountRequired: [],
      },
    },
    commitment: CommitmentLevel.PROCESSED,
  },
  (update) => {
    if (update.transaction) {
      const sig = Buffer.from(update.transaction.transaction?.signature ?? []).toString('hex');
      console.log('Transaction:', sig, 'slot:', update.transaction.slot);
    }
  },
);
```

### Subscribe to blocks

```typescript
import { subscribeBlocks } from '@solstice/solstream-sdk';

const stream = await subscribeBlocks(
  { endpoint: 'https://stream.example.com', apiKey: 'YOUR_API_KEY' },
  {
    includeTransactions: true,
    includeAccounts: false,
    includeEntries: false,
  },
  (blockUpdate) => {
    console.log(
      'Block slot:', blockUpdate.block?.slot,
      '| txns:', blockUpdate.transactions.length,
    );
  },
);
```

---

## Configuration

| Option | Type | Default | Description |
|---|---|---|---|
| `endpoint` | `string` | required | gRPC endpoint URL |
| `apiKey` | `string` | — | Sent as `x-api-key` gRPC metadata |
| `maxReconnectAttempts` | `number` | `Infinity` | Give up after N failures |
| `baseReconnectDelayMs` | `number` | `1000` | Initial back-off delay (ms) |
| `maxReconnectDelayMs` | `number` | `30000` | Maximum back-off delay (ms) |
| `channelOptions` | `ChannelOptions` | — | Extra `@grpc/grpc-js` channel options |
| `replay` | `boolean` | `false` | Resume from last processed slot on reconnect |

---

## StreamHandle

Both `subscribe()` and `subscribeBlocks()` return a `StreamHandle`:

| Member | Description |
|---|---|
| `id` | Unique UUID for this stream |
| `cancel()` | Stop the stream and disable reconnection |
| `write(request)` | Push an updated `SubscribeRequest` on the live stream (subscribe only) |

---

## Building from source

```bash
cd js
npm install
npm run build
```

---

## Examples

```bash
npm run example:accounts
npm run example:transactions
npm run example:blocks
npm run example:slots
```

Set `SOLSTREAM_ENDPOINT` and `SOLSTREAM_API_KEY` environment variables before running.
