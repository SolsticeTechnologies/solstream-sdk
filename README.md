# Solstream SDK

Real-time Solana data streaming SDKs powered by **Solstream** — the high-performance gRPC streaming infrastructure from [Solstice Staking](https://solstice.finance).

## SDKs

| Language | Directory | Status |
|---|---|---|
| TypeScript / JavaScript | [`js/`](./js) | Active |
| Rust | [`rust/`](./rust) | Coming soon |

---

## Overview

Solstream exposes a gRPC service (`StreamingService`) with two RPCs:

| RPC | Description |
|---|---|
| `Subscribe` | Stream accounts, slots, transactions, and block metadata with fine-grained filters |
| `SubscribeBlocks` | Stream full block updates including transactions, accounts, and entries |

Both RPCs support:
- **Commitment levels** — `PROCESSED`, `CONFIRMED`, `FINALIZED`
- **Slot replay** — replay historical data up to ~3000 slots back via `from_slot`

---

## Test App

[`test-app/`](./test-app) contains a CLI app for validating the SDK against a live endpoint. It includes one-shot tests and long-running monitors that stream indefinitely. See [`test-app/README.md`](./test-app/README.md) for setup and usage.

---

## JavaScript / TypeScript

See [`js/README.md`](./js/README.md) for full documentation.

### Quick install

```bash
npm install @solstice/solstream-sdk
```

### Quick start

```typescript
import { subscribe, CommitmentLevel } from '@solstice/solstream-sdk';

const stream = await subscribe(
  { endpoint: 'https://stream.example.com', apiKey: 'YOUR_KEY' },
  {
    transactions: {
      all: { vote: false, failed: false },
    },
    commitment: CommitmentLevel.CONFIRMED,
  },
  (update) => console.log(update),
);
```

---

## Rust

The `rust/` directory is reserved for a future native Rust implementation.

---

## Protocol

The full gRPC proto definitions live in [`js/proto/`](./js/proto/):

- [`streaming.proto`](./js/proto/streaming.proto) — `StreamingService` definition
- [`geyser.proto`](./js/proto/geyser.proto) — Solana Geyser data types

---

## License

MIT © Solstice Staking
