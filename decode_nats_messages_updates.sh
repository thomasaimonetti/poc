#!/bin/bash

echo "🔍 NATS Message Monitor with Auto-Decoding"
echo "=========================================="
echo "Monitoring users.update with automatic payload decoding..."
echo "Press Ctrl+C to stop"
echo ""

./internal/nats subscribe users.push.* | while IFS= read -r line; do
    echo "$line"
    
    # Check if line contains a Payload field
    if [[ $line == *"\"Payload\":"* ]]; then
        # Extract the base64 payload
        payload=$(echo "$line" | grep -o '"Payload":"[^"]*"' | cut -d'"' -f4)
        
        if [[ -n "$payload" ]]; then
            echo "🔓 Decoded Payload:"
            echo "$payload" | base64 -d | jq . 2>/dev/null || echo "   Failed to decode payload"
            echo ""
        fi
    fi
done