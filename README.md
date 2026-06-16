# 📁 File Sync

[![Go](https://img.shields.io/badge/Go-1.20+-00ADD8?logo=go&logoColor=white)](https://go.dev)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Platform](https://img.shields.io/badge/Platform-Linux-FCC624?logo=linux&logoColor=black)](https://kernel.org)

A **bidirectional file synchronization** tool written in Go. Keeps two directories in sync between a server and one or more clients over a custom binary TCP protocol — in real time.

## ✨ Features

- **Bidirectional sync** — changes on either side propagate to the other automatically
- **Real-time monitoring** — uses `inotify` (via [fsnotify](https://github.com/fsnotify/fsnotify)) to detect file changes instantly
- **Initial merge** — pre-existing files on both sides are merged on first connect (no data loss)
- **Offline resilience** — changes made while disconnected are synced upon reconnection
- **Tombstone tracking** — deletions are persisted to disk so they survive server restarts
- **Auto-reconnect** — client automatically reconnects with exponential backoff (1s → 60s)
- **Multi-client** — server supports up to 50 concurrent clients (configurable)
- **Metadata preservation** — syncs content, timestamps, permissions, and ownership
- **Conflict resolution** — last-write-wins based on modification time
- **Chunked transfers** — large files are streamed in chunks to avoid excessive memory usage
- **Lightweight protocol** — custom length-prefixed binary frames with JSON payloads over TCP

## 📋 Requirements

- **Go 1.20+**
- **Linux** (uses inotify for filesystem monitoring)

## 🔨 Build

```bash
go build -o file-sync-server ./cmd/server/
go build -o file-sync-client ./cmd/client/
```

## 🚀 Usage

### Start the Server

```bash
./file-sync-server -addr :8765 -dir /data/sync
```

| Flag | Description | Default |
|------|-------------|---------|
| `-addr` | Listen address (host:port) | `:8765` |
| `-dir` | Directory to sync | `/data/sync` |

### Start a Client

```bash
./file-sync-client -server 192.168.1.100:8765 -dir /home/user/sync
```

| Flag | Description | Default |
|------|-------------|---------|
| `-server` | Server address (host:port) | `127.0.0.1:8765` |
| `-dir` | Local directory to sync | `/home/user/sync` |
| `-id` | Client ID (auto-generated if empty) | `""` |

### Quick Local Test

```bash
# Terminal 1 — start server
./file-sync-server -addr :8765 -dir /tmp/sync-server

# Terminal 2 — start client
./file-sync-client -server 127.0.0.1:8765 -dir /tmp/sync-client

# Terminal 3 — test it
echo "hello from server" > /tmp/sync-server/test.txt    # appears on client
echo "hello from client" > /tmp/sync-client/test2.txt   # appears on server
rm /tmp/sync-server/test.txt                             # deleted on client too
```

## 🏗️ Architecture

```
┌──────────────────┐         TCP          ┌──────────────────┐
│     Client       │◄───────────────────►│     Server       │
│                  │   Binary Protocol    │                  │
│  ┌────────────┐  │                      │  ┌────────────┐  │
│  │  Monitor   │  │   HELLO / ACK        │  │  Monitor   │  │
│  │ (inotify)  │  │   SYNC_INIT          │  │ (inotify)  │  │
│  └─────┬──────┘  │   SYNC_APPLY         │  └─────┬──────┘  │
│        │         │   FILE_DATA           │        │         │
│  ┌─────▼──────┐  │   FILE_CREATE         │  ┌─────▼──────┐  │
│  │   Engine   │  │   FILE_MODIFY         │  │   Engine   │  │
│  │ (diff/     │  │   FILE_DELETE          │  │ (diff/     │  │
│  │  merge)    │  │   PING / PONG         │  │  merge)    │  │
│  └─────┬──────┘  │                      │  └─────┬──────┘  │
│        │         │                      │        │         │
│  ┌─────▼──────┐  │                      │  ┌─────▼──────┐  │
│  │ Local Dir  │  │                      │  │ Server Dir │  │
│  └────────────┘  │                      │  └────────────┘  │
└──────────────────┘                      └──────────────────┘
```

### Sync Flow

1. **Handshake** — Client sends `HELLO`, server responds with `HELLO_ACK`
2. **Tree Exchange** — Both sides exchange `SYNC_INIT` messages containing their file trees
3. **Diff & Apply** — Server computes differences and sends `SYNC_APPLY` with operations; client applies them, then sends its unique files back
4. **Real-time Sync** — Both sides monitor their directories; changes are sent as `FILE_CREATE`, `FILE_MODIFY`, or `FILE_DELETE` messages with file data chunks

### Conflict Resolution

When the same file is modified on both sides, the version with the **most recent modification time wins** (last-write-wins strategy).

## 📡 Protocol

Length-prefixed binary frames over TCP:

```
┌──────────┬──────────────┬─────────────────┐
│  Type    │   Length     │    Payload      │
│  1 byte  │  4 bytes BE  │    JSON         │
└──────────┴──────────────┴─────────────────┘
```

### Message Types

| Code | Type | Description |
|------|------|-------------|
| `0x01` | `HELLO` | Client handshake initiation |
| `0x02` | `HELLO_ACK` | Server handshake response |
| `0x03` | `SYNC_INIT` | File tree exchange for initial sync |
| `0x05` | `FILE_CREATE` | New file notification |
| `0x06` | `FILE_DATA` | File content chunk (streamed) |
| `0x07` | `FILE_MODIFY` | File modification notification |
| `0x08` | `FILE_DELETE` | File deletion notification |
| `0x09` | `FILE_MOVE` | File move/rename notification |
| `0x0A` | `ACK` | Operation acknowledgment |
| `0x0C` | `SYNC_APPLY` | Batch of sync operations |
| `0x0D` | `PING` | Keepalive ping |
| `0x0E` | `PONG` | Keepalive pong |
| `0x0F` | `ERROR` | Error notification |

Max frame size: **16 MB**

## 📂 Project Structure

```
.
├── cmd/
│   ├── server/main.go        # Server entrypoint
│   └── client/main.go        # Client entrypoint
├── protocol/
│   ├── message.go            # Message types & data structures
│   ├── codec.go              # Binary frame encode/decode
│   └── json.go               # JSON serialization helpers
├── sync/
│   ├── engine.go             # Sync engine: tree management, apply changes
│   ├── state.go              # FileTree: in-memory file state
│   ├── diff.go               # Diff algorithm (initial & incremental)
│   └── monitor.go            # Filesystem watcher (inotify)
├── netlayer/
│   ├── server.go             # TCP server & client session management
│   ├── client.go             # TCP client with reconnection
│   └── stream.go             # Connection wrapper with protocol I/O
├── config/
│   └── config.go             # Configuration structs & defaults
├── utils/
│   ├── hash.go               # SHA-256 file hashing
│   └── fs.go                 # Directory walking & file utilities
├── tests/
│   └── integration_test.sh   # End-to-end integration tests
├── go.mod
└── go.sum
```

## 🧪 Testing

Run the full integration test suite:

```bash
# Build first
go build -o file-sync-server ./cmd/server/
go build -o file-sync-client ./cmd/client/

# Run tests
bash tests/integration_test.sh
```

The integration tests cover:

| Test | Scenario |
|------|----------|
| 1 | Create file on server → syncs to client |
| 2 | Create file on client → syncs to server |
| 3 | Modify file on server → syncs to client |
| 4 | Modify file on client → syncs to server |
| 5 | Create directory with nested files → syncs |
| 6 | Delete file on server → deleted on client |
| 7 | Append to file on server → syncs to client |
| 8 | Offline changes sync on reconnect |
| 9 | Pre-existing files merge on first connect |
| 10 | Offline deletion persists after reconnect |
| 11 | Server restart preserves deletions (tombstones) |
| 12 | Server restart — client auto-reconnects |
| 13 | Client restart — server accepts reconnection |
| 14 | Multiple server restarts — client keeps reconnecting |
| 15 | Multiple client restarts — server handles gracefully |

## ⚙️ Configuration Defaults

### Server

| Parameter | Default | Description |
|-----------|---------|-------------|
| Listen Address | `:8765` | TCP listen address |
| Sync Directory | `/data/sync` | Server-side sync directory |
| Max Clients | `50` | Maximum concurrent client connections |
| Log Level | `info` | Logging verbosity |

### Client

| Parameter | Default | Description |
|-----------|---------|-------------|
| Server Address | `127.0.0.1:8765` | Server to connect to |
| Local Directory | `/home/user/sync` | Client-side sync directory |
| Reconnect | `true` | Auto-reconnect on disconnect |
| Max Reconnect Delay | `60s` | Maximum backoff between reconnection attempts |

## 📄 License

[MIT](LICENSE)
