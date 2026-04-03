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

One-shot tests — each runs for `TEST_DURATION_SECS` seconds then exits.

```bash
npm run test:slots         # Slot updates (~400ms/slot)
npm run test:transactions  # Non-vote transactions
npm run test:blocks        # Full block updates (with transactions)
npm run test:accounts      # All account updates
npm run test:all           # Run all four sequentially
```

## Run monitors

Long-running streams — connect once and stream indefinitely. Stop with `Ctrl+C`.

```bash
npm run monitor:slots
npm run monitor:transactions
npm run monitor:blocks
npm run monitor:accounts
```

## Run with Docker

All four monitors running continuously in separate containers:

```bash
# Build and start all monitors
docker compose up --build

# Detached (background)
docker compose up --build -d
docker compose logs -f

# Single monitor only
docker compose up --build monitor-slots
```

Each container uses `restart: unless-stopped` — it will automatically recover if the process crashes or the stream drops.

## Options

| Variable | Default | Description |
|---|---|---|
| `SOLSTREAM_ENDPOINT` | required | gRPC endpoint URL |
| `SOLSTREAM_API_KEY` | — | API key (sent as `x-api-key` header) |
| `TEST_DURATION_SECS` | `30` | How long each **test** runs (monitors ignore this) |

## What you should see

**Slots** — A continuous stream of slot status changes (PROCESSED → CONFIRMED → FINALIZED), roughly one slot every 400ms.

**Transactions** — Every non-vote transaction landing on chain, with signature, slot, fee, and compute units.

**Blocks** — One update per block, with transaction count and total fees.

**Accounts** — Every account write, with pubkey, lamport balance, and data size.

If any stream shows no output for >5 seconds, check your endpoint and API key.
