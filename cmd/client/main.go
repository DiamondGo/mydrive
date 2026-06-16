package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"file-sync/config"
	"file-sync/netlayer"
	"file-sync/protocol"
	"file-sync/sync"
	"file-sync/utils"
)

func main() {
	addr := flag.String("server", "127.0.0.1:8765", "server address")
	dir := flag.String("dir", "/home/user/sync", "local sync directory")
	id := flag.String("id", "", "client ID (auto-generated if empty)")
	flag.Parse()

	cfg := config.DefaultClientConfig()
	cfg.ServerAddr = *addr
	cfg.LocalDir = *dir
	cfg.ClientID = *id

	// Ensure sync directory exists
	if err := os.MkdirAll(cfg.LocalDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "create sync dir: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("file-sync client starting\n")
	fmt.Printf("  server: %s\n", cfg.ServerAddr)
	fmt.Printf("  sync dir: %s\n", cfg.LocalDir)

	// Create the TCP client (reusable across reconnections)
	client := netlayer.NewClient(cfg.ServerAddr)

	// Graceful shutdown via context
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		fmt.Println("\nshutting down...")
		cancel()
		// Close the client connection to unblock any pending reads
		client.Close()
	}()

	// Reconnection loop: each iteration does a full connect → handshake → sync → event loop cycle
	reconnectDelay := 1 * time.Second
	maxReconnectDelay := time.Duration(cfg.MaxDelaySec) * time.Second
	if maxReconnectDelay == 0 {
		maxReconnectDelay = 60 * time.Second
	}

	for {
		select {
		case <-ctx.Done():
			client.Close()
			return
		default:
		}

		err := runSession(ctx, client, cfg)
		if err == nil {
			// Normal shutdown (context cancelled)
			client.Close()
			return
		}

		// Check if shutdown was requested
		select {
		case <-ctx.Done():
			client.Close()
			return
		default:
		}

		if !cfg.Reconnect {
			fmt.Fprintf(os.Stderr, "session error (reconnect disabled): %v\n", err)
			client.Close()
			os.Exit(1)
		}

		fmt.Printf("session ended: %v\n", err)
		fmt.Printf("reconnecting in %v...\n", reconnectDelay)
		select {
		case <-time.After(reconnectDelay):
		case <-ctx.Done():
			client.Close()
			return
		}

		// Exponential backoff
		reconnectDelay = reconnectDelay * 2
		if reconnectDelay > maxReconnectDelay {
			reconnectDelay = maxReconnectDelay
		}
	}
}

// runSession performs one full session: connect → handshake → initial sync → event loop.
// It returns nil if the context was cancelled (clean shutdown), or an error if the
// connection was lost and reconnection should be attempted.
func runSession(ctx context.Context, client *netlayer.Client, cfg *config.ClientConfig) error {
	// Reuse sync engine across sessions to preserve tombstones
	engine := sync.NewEngine(cfg.LocalDir)

	// Ensure tombstones are saved on session end so they survive reconnections
	defer func() {
		if err := engine.SaveTombstones(); err != nil {
			fmt.Printf("save tombstones: %v\n", err)
		}
	}()

	// Initial scan
	if err := engine.ScanLocal(); err != nil {
		return fmt.Errorf("initial scan: %v", err)
	}
	fmt.Printf("  local tree: %d entries\n", engine.LocalTree().Len())

	// 1. Connect to server
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("connect: %v", err)
	}

	// 2. Handshake
	ack, err := client.SendHello(cfg.ClientID, cfg.LocalDir, "1.0.0")
	if err != nil {
		client.Close()
		return fmt.Errorf("handshake: %v", err)
	}
	fmt.Printf("  assigned client ID: %d\n", ack.ClientID)

	// 3. Initial sync: send our tree, receive server's tree
	localTree := engine.BuildSyncTree()
	if err := client.SendSyncInit(localTree); err != nil {
		client.Close()
		return fmt.Errorf("send sync init: %v", err)
	}

	// Receive server's tree
	msg, err := client.Receive()
	if err != nil {
		client.Close()
		return fmt.Errorf("receive sync init: %v", err)
	}

	if msg.Type == protocol.MSG_SYNC_INIT {
		engine.SetRemoteTree(msg.SyncInit)

		// Receive server's SYNC_APPLY (always sent, may be empty)
		syncMsg, err := client.Receive()
		if err != nil {
			client.Close()
			return fmt.Errorf("receive sync apply: %v", err)
		}
		if syncMsg.Type == protocol.MSG_SYNC_APPLY {
			if len(syncMsg.SyncApply.Ops) > 0 {
				fmt.Printf("  applying %d ops from server\n", len(syncMsg.SyncApply.Ops))
				if err := engine.ApplyChanges(syncMsg.SyncApply.Ops); err != nil {
					fmt.Printf("  apply changes: %v\n", err)
				}
			}
		}

		// Receive FILE_DATA chunks and ACK (sync complete signal)
		for {
			dataMsg, err := client.Receive()
			if err != nil {
				client.Close()
				return fmt.Errorf("receive initial sync data: %v", err)
			}
			if dataMsg.Type == protocol.MSG_ACK {
				// Initial sync from server is complete
				break
			}
			if dataMsg.Type == protocol.MSG_FILE_DATA {
				if err := handleServerMessage(dataMsg, engine, client, nil); err != nil {
					fmt.Printf("  handle file data: %v\n", err)
				}
			} else {
				fmt.Printf("  unexpected message 0x%02x during initial sync\n", dataMsg.Type)
			}
		}

		// Rescan local after applying server changes
		engine.ScanLocal()

		// Send our unique files to the server
		toSend := sync.DiffInitial(engine.LocalTree(), engine.RemoteTree())
		if len(toSend) > 0 {
			fmt.Printf("  %d changes to send to server\n", len(toSend))
			for _, c := range toSend {
				handleLocalEvent(c, engine, client)
			}
		}

		// Send delete messages for files the server has but the client
		// has previously deleted (tombstoned). This prevents the server
		// from re-sending these files on subsequent syncs.
		tombstones := engine.Tombstones()
		if len(tombstones) > 0 {
			serverPaths := engine.RemoteTree().Paths()
			deleteSent := 0
			for path := range tombstones {
				if serverPaths[path] {
					// Server still has this file, tell it to delete
					if err := client.SendFileDelete(path); err != nil {
						fmt.Printf("  send tombstone delete: %v\n", err)
					} else {
						deleteSent++
					}
				}
			}
			if deleteSent > 0 {
				fmt.Printf("  sent %d tombstone deletions to server\n", deleteSent)
			}
			// Clear tombstones after communicating them
			engine.ClearTombstones()
		}
	}

	// 4. Start file monitor for local changes
	monitor, err := sync.NewMonitor(cfg.LocalDir)
	if err != nil {
		client.Close()
		return fmt.Errorf("create monitor: %v", err)
	}
	monitor.Start()
	defer monitor.Stop()

	// Handle local file changes in a goroutine
	// Use a session-scoped context so this goroutine stops when the session ends
	sessionCtx, sessionCancel := context.WithCancel(ctx)
	defer sessionCancel()

	go func() {
		for {
			select {
			case <-sessionCtx.Done():
				return
			case event, ok := <-monitor.Events():
				if !ok {
					return
				}
				handleLocalEvent(event, engine, client)
			}
		}
	}()

	// 5. Main receive loop
	fmt.Println("syncing...")
	for {
		select {
		case <-sessionCtx.Done():
			client.Close()
			return nil // Clean shutdown
		default:
		}

		msg, err := client.Receive()
		if err != nil {
			// Check if shutdown was requested
			select {
			case <-sessionCtx.Done():
				client.Close()
				return nil
			default:
			}
			client.Close()
			return fmt.Errorf("read: %v", err)
		}

		if err := handleServerMessage(msg, engine, client, monitor); err != nil {
			fmt.Printf("handle: %v\n", err)
		}
	}
}

func handleLocalEvent(event *protocol.ChangeEvent, engine *sync.Engine, client *netlayer.Client) {
	switch event.Type {
	case "create":
		if event.Entry != nil {
			if err := client.SendFileCreate(event.Entry); err != nil {
				fmt.Printf("send create: %v\n", err)
				return
			}
			fmt.Printf("  uploaded: %s\n", event.Entry.Path)
			if event.Entry.EntryType == protocol.FileTypeRegularFile {
				fullPath := filepath.Join(engine.BaseDir(), event.Entry.Path)
				if err := client.SendFileContent(fullPath, event.Entry.Path); err != nil {
					fmt.Printf("send content: %v\n", err)
				}
			}
		}
	case "modify":
		if event.Entry != nil {
			if err := client.SendFileModify(event.Entry); err != nil {
				fmt.Printf("send modify: %v\n", err)
				return
			}
			fmt.Printf("  modified: %s\n", event.Entry.Path)
			if event.Entry.EntryType == protocol.FileTypeRegularFile {
				fullPath := filepath.Join(engine.BaseDir(), event.Entry.Path)
				if err := client.SendFileContent(fullPath, event.Entry.Path); err != nil {
					fmt.Printf("send content: %v\n", err)
				}
			}
		}
	case "delete":
		if err := client.SendFileDelete(event.Path); err != nil {
			fmt.Printf("send delete: %v\n", err)
		} else {
			fmt.Printf("  deleted: %s\n", event.Path)
		}
	}
}

func handleServerMessage(msg *protocol.Message, engine *sync.Engine, client *netlayer.Client, monitor *sync.Monitor) error {
	switch msg.Type {
	case protocol.MSG_FILE_CREATE:
		fmt.Printf("  server create: %s\n", msg.FileCreate.Path)
		// Ignore in monitor to prevent feedback loop
		if monitor != nil {
			monitor.Ignore(msg.FileCreate.Path, 2*time.Second)
		}
		return nil

	case protocol.MSG_FILE_MODIFY:
		fmt.Printf("  server modify: %s\n", msg.FileModify.Path)
		// Ignore in monitor to prevent feedback loop
		if monitor != nil {
			monitor.Ignore(msg.FileModify.Path, 2*time.Second)
		}
		return nil

	case protocol.MSG_FILE_DATA:
		chunk := msg.FileData
		fullPath := filepath.Join(engine.BaseDir(), chunk.Path)
		// Ignore in monitor to prevent feedback loop
		if monitor != nil {
			monitor.Ignore(chunk.Path, 2*time.Second)
		}
		_ = utils.EnsureDir(fullPath)
		if err := utils.WriteChunkAt(fullPath, chunk.Data, int64(chunk.Offset)); err != nil {
			return fmt.Errorf("write chunk: %w", err)
		}
		return nil

	case protocol.MSG_FILE_DELETE:
		fmt.Printf("  server delete: %s\n", msg.FileDelete)
		// Ignore in monitor to prevent the deletion from being sent back
		// to the server as a new delete event
		if monitor != nil {
			monitor.Ignore(msg.FileDelete, 2*time.Second)
		}
		path := filepath.Join(engine.BaseDir(), msg.FileDelete)
		if err := utils.DeleteEntry(path); err != nil {
			return fmt.Errorf("delete file: %w", err)
		}
		engine.RemoteTree().Remove(msg.FileDelete)
		engine.ScanLocal()
		return nil

	case protocol.MSG_SYNC_APPLY:
		if err := engine.ApplyChanges(msg.SyncApply.Ops); err != nil {
			return fmt.Errorf("apply ops: %w", err)
		}
		fmt.Printf("  applied %d ops from server\n", len(msg.SyncApply.Ops))
		return nil

	case protocol.MSG_PING:
		return client.SendPong()

	default:
		return nil
	}
}

func buildSyncOps(changes []*protocol.ChangeEvent) []*protocol.SyncOp {
	var ops []*protocol.SyncOp
	for _, c := range changes {
		switch c.Type {
		case "create":
			if c.Entry != nil {
				if c.Entry.EntryType == protocol.FileTypeDirectory {
					ops = append(ops, &protocol.SyncOp{Type: "createDir", Entry: c.Entry})
				} else {
					ops = append(ops, &protocol.SyncOp{Type: "createFile", Entry: c.Entry})
				}
			}
		case "modify":
			if c.Entry != nil {
				ops = append(ops, &protocol.SyncOp{Type: "setAttrs", Path: c.Entry.Path, Entry: c.Entry})
			}
		case "delete":
			ops = append(ops, &protocol.SyncOp{Type: "delete", Path: c.Path})
		}
	}
	return ops
}
