#!/bin/bash

echo "🔍 NATS Message Monitor"
echo "======================"

echo "📊 Stream Information:"
./internal/nats stream info USERS_UPDATE

echo ""
echo "📡 Subscribing to users.update messages..."
echo "   (Press Ctrl+C to stop)"
echo ""

# Subscribe to messages and display them
./internal/nats subscribe USERS_UPDATE --count=10