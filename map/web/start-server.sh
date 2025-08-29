#!/bin/bash

# Kill any existing server on port 3000
echo "🛑 Stopping any existing server..."
lsof -ti:3000 | xargs kill -9 2>/dev/null || true

# Wait a moment for cleanup
sleep 2

# Start server in background
echo "🚀 Starting geospatial web server..."
nohup npm run dev > server.log 2>&1 &

# Wait for server to start
sleep 5

# Test if server is healthy
echo "🔍 Testing server health..."
if curl -s http://localhost:3000/api/health > /dev/null; then
    echo "✅ Server running successfully on localhost:3000"
    echo "📊 Server logs: tail -f server.log"
else
    echo "❌ Server failed to start - check server.log"
fi