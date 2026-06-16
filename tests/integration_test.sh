#!/bin/bash

set -e

# Test directories
# Assume script is run from the workspace root or from the file-sync dir
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
WORKSPACE=$(dirname "$SCRIPT_DIR")
if [ "$WORKSPACE" == "." ] || [ "$WORKSPACE" == "/" ]; then
    WORKSPACE="file-sync"
fi

TEST_ROOT="$WORKSPACE/tests/data"
SERVER_DIR="$TEST_ROOT/server"
CLIENT_DIR="$TEST_ROOT/client"
SERVER_BIN="$WORKSPACE/file-sync-server"
CLIENT_BIN="$WORKSPACE/file-sync-client"
ADDR="127.0.0.1:8765"

# Cleanup function
cleanup() {
    echo "Cleaning up..."
    kill $(jobs -p) 2>/dev/null || true
    rm -rf "$TEST_ROOT"
}

trap cleanup EXIT

verify_sync() {
    local file=$1
    echo "Verifying $file..."
    if diff "$SERVER_DIR/$file" "$CLIENT_DIR/$file" > /dev/null; then
        echo "✅ Synced: $file"
    else
        echo "❌ Not synced: $file"
        exit 1
    fi
}

verify_not_exists() {
    local file=$1
    local side=$2
    local dir
    if [ "$side" == "server" ]; then
        dir="$SERVER_DIR"
    else
        dir="$CLIENT_DIR"
    fi
    if [ ! -e "$dir/$file" ]; then
        echo "✅ $file does not exist on $side (as expected)"
    else
        echo "❌ $file still exists on $side (should have been deleted)"
        exit 1
    fi
}

# ============================================================
# Test 9: Pre-existing files on either side sync on connect
# ============================================================
echo "=========================================="
echo "Test 9: Pre-existing files sync on connect"
echo "=========================================="

# Setup directories with pre-existing files BEFORE starting server/client
rm -rf "$TEST_ROOT"
mkdir -p "$SERVER_DIR"
mkdir -p "$CLIENT_DIR"

# Create files that only exist on server
echo "server-only content" > "$SERVER_DIR/server_preexist.txt"
mkdir -p "$SERVER_DIR/server_subdir"
echo "server subdir file" > "$SERVER_DIR/server_subdir/nested.txt"

# Create files that only exist on client
echo "client-only content" > "$CLIENT_DIR/client_preexist.txt"
mkdir -p "$CLIENT_DIR/client_subdir"
echo "client subdir file" > "$CLIENT_DIR/client_subdir/nested.txt"

# Create a file that exists on both with same content (should not cause issues)
echo "shared content" > "$SERVER_DIR/shared.txt"
echo "shared content" > "$CLIENT_DIR/shared.txt"

echo "Pre-existing files created."
echo "  Server: server_preexist.txt, server_subdir/nested.txt, shared.txt"
echo "  Client: client_preexist.txt, client_subdir/nested.txt, shared.txt"

# Start server
echo "Starting server..."
$SERVER_BIN -addr $ADDR -dir "$SERVER_DIR" &
SERVER_PID=$!
sleep 1

# Start client
echo "Starting client..."
$CLIENT_BIN -server $ADDR -dir "$CLIENT_DIR" &
CLIENT_PID=$!
sleep 3

# Verify: server-only files should now exist on client
echo "Checking server→client sync..."
verify_sync "server_preexist.txt"
verify_sync "server_subdir/nested.txt"

# Verify: client-only files should now exist on server
echo "Checking client→server sync..."
verify_sync "client_preexist.txt"
verify_sync "client_subdir/nested.txt"

# Verify: shared file still matches
echo "Checking shared file..."
verify_sync "shared.txt"

echo "✅ Test 9 passed: Pre-existing files synced on connect"
echo ""

# ============================================================
# Tests 1-8 (original tests, using the already-running server/client)
# ============================================================

# 1. Create file on server
echo "Test 1: Create file on server"
echo "hello server" > "$SERVER_DIR/test1.txt"
sleep 1
verify_sync "test1.txt"

# 2. Create file on client
echo "Test 2: Create file on client"
echo "hello client" > "$CLIENT_DIR/test2.txt"
sleep 1
verify_sync "test2.txt"

# 3. Modify file on server
echo "Test 3: Modify file on server"
echo "modified by server" > "$SERVER_DIR/test1.txt"
sleep 1
verify_sync "test1.txt"

# 4. Modify file on client
echo "Test 4: Modify file on client"
echo "modified by client" > "$CLIENT_DIR/test2.txt"
sleep 1
verify_sync "test2.txt"

# 5. Create directory and file in it on server
echo "Test 5: Create dir on server"
mkdir -p "$SERVER_DIR/subdir"
echo "in subdir" > "$SERVER_DIR/subdir/file.txt"
sleep 1
if [ -f "$CLIENT_DIR/subdir/file.txt" ]; then
    echo "✅ Dir and file synced"
    verify_sync "subdir/file.txt"
else
    echo "❌ Dir not synced"
    exit 1
fi

# 6. Delete file on server
echo "Test 6: Delete file on server"
rm "$SERVER_DIR/test1.txt"
sleep 1
if [ ! -f "$CLIENT_DIR/test1.txt" ]; then
    echo "✅ Deletion synced"
else
    echo "❌ Deletion not synced"
    exit 1
fi

# 7. Append to file on server
echo "Test 7: Append to file on server"
echo "appended line" >> "$SERVER_DIR/test2.txt"
sleep 1
verify_sync "test2.txt"

# 8. Offline synchronization (new files)
echo "Test 8: Offline sync"
# Stop client
kill $CLIENT_PID
wait $CLIENT_PID 2>/dev/null || true
echo "Client stopped"

# Modify file while offline
echo "offline modification" > "$SERVER_DIR/offline.txt"
echo "update existing" >> "$SERVER_DIR/test2.txt"
echo "Offline changes made on server"

# Start client again
echo "Starting client again..."
$CLIENT_BIN -server $ADDR -dir "$CLIENT_DIR" &
CLIENT_PID=$!
sleep 2

verify_sync "offline.txt"
verify_sync "test2.txt"

echo "✅ Test 8 passed"
echo ""

# ============================================================
# Test 10: Offline deletion stays deleted after reconnect
# ============================================================
echo "=========================================="
echo "Test 10: Offline deletion stays deleted"
echo "=========================================="

# First, make sure a file exists on both sides
echo "delete-me content" > "$SERVER_DIR/delete_test.txt"
sleep 2
verify_sync "delete_test.txt"

# Stop client
kill $CLIENT_PID
wait $CLIENT_PID 2>/dev/null || true
echo "Client stopped for offline deletion test"

# Delete the file on the server while client is offline
rm -f "$SERVER_DIR/delete_test.txt"
echo "Deleted delete_test.txt on server while client offline"

# Start client again
echo "Starting client again..."
$CLIENT_BIN -server $ADDR -dir "$CLIENT_DIR" &
CLIENT_PID=$!
sleep 3

# The file should be deleted on BOTH sides
# Since the server deleted it, the client should also delete it
verify_not_exists "delete_test.txt" "server"
verify_not_exists "delete_test.txt" "client"

echo "✅ Test 10 passed: Offline deletion stayed deleted"
echo ""

# ============================================================
# Test 11: Server restart with tombstone persistence
# ============================================================
echo "=========================================="
echo "Test 11: Server restart preserves deletions"
echo "=========================================="

# Stop both client and server
kill $CLIENT_PID
wait $CLIENT_PID 2>/dev/null || true
kill $SERVER_PID
wait $SERVER_PID 2>/dev/null || true
echo "Both server and client stopped"

# Create a file on server, then delete it while nothing is running
echo "restart-delete-test" > "$SERVER_DIR/restart_del.txt"
# Start server briefly to register the file
$SERVER_BIN -addr $ADDR -dir "$SERVER_DIR" &
SERVER_PID=$!
sleep 1
# Stop server
kill $SERVER_PID
wait $SERVER_PID 2>/dev/null || true
echo "Server stopped after registering restart_del.txt"

# Delete the file while server is stopped
rm -f "$SERVER_DIR/restart_del.txt"
echo "Deleted restart_del.txt while server is stopped"

# Copy the file to client (simulating what would have been synced)
echo "restart-delete-test" > "$CLIENT_DIR/restart_del.txt"

# Start server again (should load tombstones from disk)
echo "Starting server again..."
$SERVER_BIN -addr $ADDR -dir "$SERVER_DIR" &
SERVER_PID=$!
sleep 1

# Start client
echo "Starting client..."
$CLIENT_BIN -server $ADDR -dir "$CLIENT_DIR" &
CLIENT_PID=$!
sleep 3

# The file should be deleted on both sides
verify_not_exists "restart_del.txt" "server"
verify_not_exists "restart_del.txt" "client"

# Verify tombstone file is cleaned up after sync
if [ ! -f "$SERVER_DIR/.sync-tombstones" ]; then
    echo "✅ Tombstone file cleaned up after sync"
else
    echo "⚠️  Tombstone file still exists (acceptable if other tombstones remain)"
fi

echo "✅ Test 11 passed: Server restart preserves deletions via tombstones"
echo ""

# ============================================================
# Test 12: Server restart — client auto-reconnects and syncs
# ============================================================
echo "=========================================="
echo "Test 12: Server restart — client reconnects"
echo "=========================================="

# Make sure server and client are running from previous test
# (they should be, but let's be safe)
kill $CLIENT_PID 2>/dev/null; wait $CLIENT_PID 2>/dev/null || true
kill $SERVER_PID 2>/dev/null; wait $SERVER_PID 2>/dev/null || true

# Clean up test dirs
rm -rf "$TEST_ROOT"
mkdir -p "$SERVER_DIR" "$CLIENT_DIR"

# Create initial file on server
echo "before-restart" > "$SERVER_DIR/restart_test.txt"

# Start server
echo "Starting server..."
$SERVER_BIN -addr $ADDR -dir "$SERVER_DIR" &
SERVER_PID=$!
sleep 1

# Start client
echo "Starting client..."
$CLIENT_BIN -server $ADDR -dir "$CLIENT_DIR" &
CLIENT_PID=$!
sleep 3

# Verify initial sync
verify_sync "restart_test.txt"
echo "Initial sync OK"

# Kill the server (simulating server crash/restart)
echo "Killing server (simulating crash)..."
kill $SERVER_PID
wait $SERVER_PID 2>/dev/null || true
echo "Server stopped"

# Wait a moment for client to detect disconnection
sleep 2

# Create a new file on server dir while server is down
echo "new-after-restart" > "$SERVER_DIR/after_restart.txt"

# Restart the server
echo "Restarting server..."
$SERVER_BIN -addr $ADDR -dir "$SERVER_DIR" &
SERVER_PID=$!
sleep 1

# Wait for client to reconnect and re-sync
# Client has exponential backoff starting at 1s, so give it time
sleep 8

# Verify the client got the new file that was created while server was down
verify_sync "after_restart.txt"

# Create another file on client AFTER reconnection to verify bidirectional sync works
echo "client-after-reconnect" > "$CLIENT_DIR/client_reconnected.txt"
sleep 2
verify_sync "client_reconnected.txt"

# Create a file on server AFTER reconnection
echo "server-after-reconnect" > "$SERVER_DIR/server_reconnected.txt"
sleep 2
verify_sync "server_reconnected.txt"

echo "✅ Test 12 passed: Client reconnected after server restart and sync works"
echo ""

# ============================================================
# Test 13: Client restart — server accepts new connection
# ============================================================
echo "=========================================="
echo "Test 13: Client restart — server accepts reconnection"
echo "=========================================="

# Kill client (simulating client crash/restart)
echo "Killing client (simulating crash)..."
kill $CLIENT_PID
wait $CLIENT_PID 2>/dev/null || true
echo "Client stopped"

# Create a file on server while client is down
echo "while-client-down" > "$SERVER_DIR/client_was_down.txt"
sleep 1

# Restart client
echo "Restarting client..."
$CLIENT_BIN -server $ADDR -dir "$CLIENT_DIR" &
CLIENT_PID=$!
sleep 3

# Verify the file created while client was down got synced
verify_sync "client_was_down.txt"

# Verify bidirectional sync still works after client restart
echo "client-restarted-file" > "$CLIENT_DIR/after_client_restart.txt"
sleep 2
verify_sync "after_client_restart.txt"

echo "server-after-client-restart" > "$SERVER_DIR/server_after_client_restart.txt"
sleep 2
verify_sync "server_after_client_restart.txt"

echo "✅ Test 13 passed: Server accepted client reconnection and sync works"
echo ""

# ============================================================
# Test 14: Multiple server restarts — client keeps reconnecting
# ============================================================
echo "=========================================="
echo "Test 14: Multiple server restarts"
echo "=========================================="

for i in 1 2 3; do
    echo "--- Restart cycle $i ---"

    # Kill server
    kill $SERVER_PID
    wait $SERVER_PID 2>/dev/null || true
    echo "  Server stopped"

    # Create a file while server is down
    echo "cycle-$i-content" > "$SERVER_DIR/cycle_${i}.txt"
    sleep 1

    # Restart server
    $SERVER_BIN -addr $ADDR -dir "$SERVER_DIR" &
    SERVER_PID=$!
    sleep 1

    # Wait for client to reconnect
    sleep 8

    # Verify sync
    verify_sync "cycle_${i}.txt"
    echo "  Cycle $i: sync OK"
done

# Verify all cycle files exist
for i in 1 2 3; do
    verify_sync "cycle_${i}.txt"
done

echo "✅ Test 14 passed: Client survived multiple server restarts"
echo ""

# ============================================================
# Test 15: Multiple client restarts — server handles gracefully
# ============================================================
echo "=========================================="
echo "Test 15: Multiple client restarts"
echo "=========================================="

for i in 1 2 3; do
    echo "--- Client restart cycle $i ---"

    # Kill client
    kill $CLIENT_PID
    wait $CLIENT_PID 2>/dev/null || true
    echo "  Client stopped"

    # Create a file on server while client is down
    echo "client-cycle-$i" > "$SERVER_DIR/client_cycle_${i}.txt"
    sleep 1

    # Restart client
    $CLIENT_BIN -server $ADDR -dir "$CLIENT_DIR" &
    CLIENT_PID=$!
    sleep 3

    # Verify sync
    verify_sync "client_cycle_${i}.txt"
    echo "  Cycle $i: sync OK"
done

echo "✅ Test 15 passed: Server handled multiple client restarts"
echo ""

echo "All integration tests passed! 🎉"


