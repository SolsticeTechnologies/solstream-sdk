#!/bin/bash
set -e

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Solstream SDK — setting up dev environment"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# 1. Install & build the SDK
echo "► Installing SDK dependencies (js/)..."
cd js
npm install
echo "► Building SDK..."
npm run build
cd ..

# 2. Install test-app dependencies (links to local SDK)
echo "► Installing test-app dependencies..."
cd test-app
npm install
cd ..

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  ✓ Setup complete!"
echo ""
echo "  Next steps:"
echo "    1. cp test-app/.env.example test-app/.env"
echo "    2. Edit test-app/.env with your endpoint + API key"
echo "    3. cd test-app && npm run test:slots"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
