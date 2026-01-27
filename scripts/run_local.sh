#!/bin/bash
# Run a middleware node on a physical machine
# Usage: ./scripts/run_local.sh <node_id>
# Example: ./scripts/run_local.sh 1

set -e

NODE_ID=${1:-1}

if [ -z "$1" ]; then
    echo "Usage: $0 <node_id>"
    echo "Example: $0 1"
    exit 1
fi

# Check if nodes.local.json exists
if [ ! -f "config/nodes.local.json" ]; then
    echo "Error: config/nodes.local.json not found!"
    echo "Copy config/nodes.local.json.example to config/nodes.local.json and update IPs."
    exit 1
fi

# Check if .env.local exists and load it
if [ -f ".env.local" ]; then
    echo "Loading environment from .env.local"
    export $(cat .env.local | grep -v '^#' | xargs)
fi

echo "Starting middleware node $NODE_ID in local mode..."
echo "Using config/nodes.local.json for node configuration"

python main.py --node-id "$NODE_ID" --local --log-level INFO
