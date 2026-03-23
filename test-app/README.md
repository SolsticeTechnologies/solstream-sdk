# Solstream Test App

CLI test application for validating the Solstream SDK against a live endpoint.

## Setup

```bash
# 1. Copy env template
cp .env.example .env

# 2. Fill in your values
#    SOLSTREAM_ENDPOINT=https://your-endpoint
#    SOLSTREAM_API_KEY=your-key

# 3. Install dependencies (SDK is linked locally)
npm install
```

## Run tests

```bash
# Slot updates (fastest feedback — ~400ms/slot)
npm run test:slots

# Non-vote transactions
npm run test:transactions

# Full block updates (with transactions)
npm run test:blocks

# All account updates
npm run test:accounts

# Run all tests sequentially
npm run test:all
```

## Options

| Variable | Default | Description |
|---|---|---|
| `SOLSTREAM_ENDPOINT` | required | gRPC endpoint URL |
| `SOLSTREAM_API_KEY` | — | API key (sent as `x-api-key` header) |
| `TEST_DURATION_SECS` | `30` | How long each test runs before auto-stopping |

## What you should see

**Slots** — A continuous stream of slot status changes (PROCESSED → CONFIRMED → FINALIZED), roughly one slot every 400ms.

**Transactions** — Every non-vote transaction landing on chain, with signature, slot, fee, and compute units.

**Blocks** — One update per block, with transaction count and total fees.

**Accounts** — Every account write, with pubkey, lamport balance, and data size.

If any test shows no output for >5 seconds, check your endpoint and API key.
