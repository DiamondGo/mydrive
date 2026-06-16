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
	addr := flag.String("addr", ":8765", "listen address")
	dir := flag.String("dir", "/data/sync", "server sync directory")
	flag.Parse()

	cfg := config.DefaultServerConfig()
	cfg.ListenAddr = *addr
	cfg.ServerDir = *dir

	// Ensure sync directory exists
	if err := os.MkdirAll(cfg.ServerDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "create sync dir: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("file-sync server starting\n")
	fmt.Printf("  listen: %s\n", cfg.ListenAddr)
	fmt.Printf("  sync dir: %s\n", cfg.ServerDir)

	// Create sync engine for server-side directory
	engine := sync.NewEngine(cfg.ServerDir)

	// Initial scan
	if err := engine.ScanLocal(); err != nil {
		fmt.Fprintf(os.Stderr, "initial scan: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("  local tree: %d entries\n", engine.LocalTree().Len())

	// Start file monitor
	monitor, err := sync.NewMonitor(cfg.ServerDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create monitor: %v\n", err)
		os.Exit(1)
	}
	monitor.Start()

	// Create TCP server
	srv := netlayer.NewServer(cfg.ListenAddr)

	// Graceful shutdown via context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start monitor event handler
	go func() {
		for event := range monitor.Events() {
			handleMonitorEvent(event, engine, srv, monitor)
		}
	}()

	// Graceful shutdown
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		fmt.Println("\nshutting down...")
		// Save tombstones and known paths before shutdown
		if err := engine.SaveTombstones(); err != nil {
			fmt.Printf("save tombstones: %v\n", err)
		}
		cancel()
		monitor.Stop()
		srv.Close()
	}()

	if err := srv.ListenAndServe(func(session *netlayer.ClientSession) {
		// Create a per-session context so monitorClientChanges stops when this client disconnects
		sessionCtx, sessionCancel := context.WithCancel(ctx)
		defer sessionCancel()
		handleClient(sessionCtx, session, engine, srv, cfg, monitor)
	}); err != nil {
		// Check if shutdown was requested
		select {
		case <-ctx.Done():
			// Normal shutdown
		default:
			fmt.Fprintf(os.Stderr, "server error: %v\n", err)
			os.Exit(1)
		}
	}
}

func handleClient(ctx context.Context, session *netlayer.ClientSession, engine *sync.Engine, srv *netlayer.Server, cfg *config.ServerConfig, monitor *sync.Monitor) {
	// Rescan local directory to ensure tombstones are up-to-date
	// (monitor events may not have fired yet)
	engine.ScanLocal()

	// 1. Handshake
	msg, err := session.Conn.ReadMessage()
	if err != nil {
		fmt.Printf("client %d: read hello: %v\n", session.ID, err)
		return
	}

	if msg.Type != protocol.MSG_HELLO {
		fmt.Printf("client %d: expected HELLO, got 0x%02x\n", session.ID, msg.Type)
		return
	}

	session.SyncDir = msg.Hello.SyncDir
	fmt.Printf("client %d: hello from %s, sync dir: %s, version: %s\n",
		session.ID, session.Conn.RemoteAddr(), msg.Hello.SyncDir, msg.Hello.Version)

	// Send HELLO_ACK
	ack := &protocol.HelloAckPayload{
		ServerID: "file-sync-server-1.0",
		ClientID: session.ID,
		Status:   0,
	}
	if err := session.Conn.WriteMessage(&protocol.Message{
		Type:     protocol.MSG_HELLO_ACK,
		HelloAck: ack,
	}); err != nil {
		fmt.Printf("client %d: send hello ack: %v\n", session.ID, err)
		return
	}

	// 2. Initial sync: send our tree, receive theirs
	localTree := engine.BuildSyncTree()
	if err := session.Conn.WriteMessage(&protocol.Message{
		Type:     protocol.MSG_SYNC_INIT,
		SyncInit: localTree,
	}); err != nil {
		fmt.Printf("client %d: send sync init: %v\n", session.ID, err)
		return
	}

	// Receive client's tree
	resp, err := session.Conn.ReadMessage()
	if err != nil {
		fmt.Printf("client %d: read sync init: %v\n", session.ID, err)
		return
	}

	if resp.Type == protocol.MSG_SYNC_INIT {
		engine.SetRemoteTree(resp.SyncInit)

		// Record client's paths as previously known so we can track deletions
		clientPaths := make([]string, 0, len(resp.SyncInit.Entries))
		for _, entry := range resp.SyncInit.Entries {
			clientPaths = append(clientPaths, entry.Path)
		}
		engine.AddToPreviousPaths(clientPaths)

		// Compute diff: what does the client need to match the server?
		// Use DiffInitial (no deletes) for creates/modifies
		changes := sync.DiffInitial(engine.LocalTree(), engine.RemoteTree())

		// Additionally, send explicit deletes for files the client has
		// but the server doesn't, IF the server previously knew about them.
		// This handles the case where the server deleted a file while the
		// client was offline.
		tombstones := engine.Tombstones()
		localPaths := engine.LocalTree().Paths()
		for _, entry := range resp.SyncInit.Entries {
			if !localPaths[entry.Path] {
				// File is on client but not on server
				if tombstones[entry.Path] {
					// Server previously had this file and deleted it
					changes = append(changes, &protocol.ChangeEvent{
						Type: "delete",
						Path: entry.Path,
					})
				}
				// else: server never had this file → client will send it
			}
		}

		// Always send SYNC_APPLY (even if empty) so client knows to proceed
		ops := buildSyncOps(changes)
		applyMsg := &protocol.Message{
			Type:      protocol.MSG_SYNC_APPLY,
			SyncApply: &protocol.SyncApply{Ops: ops},
		}
		if err := session.Conn.WriteMessage(applyMsg); err != nil {
			fmt.Printf("client %d: send sync apply: %v\n", session.ID, err)
			return
		}
		if len(changes) > 0 {
			fmt.Printf("client %d: %d changes to send to client\n", session.ID, len(changes))
		}

		// Stream file contents for new/modified files
		for _, c := range changes {
			if (c.Type == "create" || c.Type == "modify") && c.Entry != nil && c.Entry.EntryType == protocol.FileTypeRegularFile {
				fullPath := filepath.Join(engine.BaseDir(), c.Entry.Path)
				_ = utils.ReadFileChunked(fullPath, func(offset uint64, data []byte, isFinal bool) error {
					flags := byte(0)
					if isFinal {
						flags |= protocol.FlagFinalChunk
					}
					return session.Conn.WriteMessage(&protocol.Message{
						Type: protocol.MSG_FILE_DATA,
						FileData: &protocol.FileDataChunk{
							Path:   c.Entry.Path,
							Flags:  flags,
							Offset: offset,
							Data:   data,
						},
					})
				})
			}
		}

		// Send ACK to signal initial sync complete
		if err := session.Conn.WriteMessage(&protocol.Message{
			Type: protocol.MSG_ACK,
			Ack:  &protocol.AckPayload{Seq: 0, Status: 0},
		}); err != nil {
			fmt.Printf("client %d: send sync complete ack: %v\n", session.ID, err)
			return
		}

		// Clear tombstones after successful sync — they've been communicated
		engine.ClearTombstones()
	}

	// 3. Main event loop
	go monitorClientChanges(ctx, session, engine, cfg)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msg, err := session.Conn.ReadMessage()
		if err != nil {
			fmt.Printf("client %d: read: %v\n", session.ID, err)
			return
		}

		if err := handleMessage(session, msg, engine, srv, monitor); err != nil {
			fmt.Printf("client %d: handle: %v\n", session.ID, err)
			// Non-fatal errors (e.g. permission denied on chmod) should not
			// disconnect the client. Only return on I/O errors that indicate
			// the connection is broken.
		}
	}
}

func handleMessage(session *netlayer.ClientSession, msg *protocol.Message, engine *sync.Engine, srv *netlayer.Server, monitor *sync.Monitor) error {
	switch msg.Type {
	case protocol.MSG_FILE_CREATE:
		// Ignore this path in the monitor to prevent feedback loops
		monitor.Ignore(msg.FileCreate.Path, 2*time.Second)
		if err := applyFileCreate(engine.BaseDir(), msg.FileCreate); err != nil {
			return fmt.Errorf("create file: %w", err)
		}
		fmt.Printf("client %d: created %s\n", session.ID, msg.FileCreate.Path)
		// Clear any tombstone for this path since it's being re-created
		engine.ClearTombstone(msg.FileCreate.Path)
		engine.RemoteTree().Set(msg.FileCreate)
		engine.ScanLocal()
		return nil

	case protocol.MSG_FILE_MODIFY:
		// Ignore this path in the monitor to prevent feedback loops
		monitor.Ignore(msg.FileModify.Path, 2*time.Second)
		if err := applyFileModify(engine.BaseDir(), msg.FileModify); err != nil {
			return fmt.Errorf("modify file: %w", err)
		}
		engine.RemoteTree().Set(msg.FileModify)
		engine.ScanLocal()
		return nil

	case protocol.MSG_FILE_DELETE:
		path := filepath.Join(engine.BaseDir(), msg.FileDelete)
		// Ignore this path in the monitor to prevent feedback loops
		// (the monitor would detect the deletion and try to broadcast it back)
		monitor.Ignore(msg.FileDelete, 2*time.Second)
		if err := utils.DeleteEntry(path); err != nil {
			return fmt.Errorf("delete file: %w", err)
		}
		// Remove from both trees so monitorClientChanges won't re-send it
		engine.RemoteTree().Remove(msg.FileDelete)
		engine.LocalTree().Remove(msg.FileDelete)
		// Add to tombstones so it won't be re-created on next initial sync
		engine.AddTombstone(msg.FileDelete)
		engine.ScanLocal()
		// Broadcast the delete to other connected clients
		srv.Broadcast(session.ID, msg)
		return nil

	case protocol.MSG_FILE_DATA:
		chunk := msg.FileData
		fullPath := filepath.Join(engine.BaseDir(), chunk.Path)
		_ = utils.EnsureDir(fullPath)
		if err := utils.WriteChunkAt(fullPath, chunk.Data, int64(chunk.Offset)); err != nil {
			return fmt.Errorf("write chunk: %w", err)
		}
		srv.Broadcast(session.ID, msg)
		return nil

	case protocol.MSG_PING:
		return session.Conn.WriteMessage(&protocol.Message{Type: protocol.MSG_PONG})

	case protocol.MSG_ACK:
		return nil

	default:
		return nil
	}
}

func applyFileCreate(baseDir string, entry *protocol.FileEntry) error {
	path := filepath.Join(baseDir, entry.Path)
	if entry.EntryType == protocol.FileTypeDirectory {
		return os.MkdirAll(path, os.FileMode(entry.Mode))
	}
	if err := utils.EnsureDir(path); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	f.Close()
	return utils.ApplyAttrs(path, entry)
}

func applyFileModify(baseDir string, entry *protocol.FileEntry) error {
	path := filepath.Join(baseDir, entry.Path)
	return utils.ApplyAttrs(path, entry)
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

func monitorClientChanges(ctx context.Context, session *netlayer.ClientSession, engine *sync.Engine, cfg *config.ServerConfig) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			engine.ScanLocal()
			changes := sync.Diff(engine.LocalTree(), engine.RemoteTree())
			if len(changes) > 0 {
				ops := buildSyncOps(changes)
				msg := &protocol.Message{
					Type:      protocol.MSG_SYNC_APPLY,
					SyncApply: &protocol.SyncApply{Ops: ops},
				}
				if err := session.Conn.WriteMessage(msg); err != nil {
					return
				}
			}
		}
	}
}

func handleMonitorEvent(event *protocol.ChangeEvent, engine *sync.Engine, srv *netlayer.Server, monitor *sync.Monitor) {
	// Skip tombstone file changes
	if event.Path == sync.TombstoneFile {
		return
	}

	if event.Type == "create" || event.Type == "modify" {
		engine.ScanLocal()
		if event.Entry == nil {
			return
		}
		engine.RemoteTree().Set(event.Entry)
		// Ignore this path for a short while to avoid feedback loops
		monitor.Ignore(event.Path, 500*time.Millisecond)

		msg := &protocol.Message{
			Type:       protocol.MSG_FILE_MODIFY,
			FileModify: event.Entry,
		}
		clients := srv.GetClients()
		for _, c := range clients {
			if err := c.Conn.WriteMessage(msg); err != nil {
				fmt.Printf("send to client %d: %v\n", c.ID, err)
				continue
			}
			// Also stream the file content if it's a file
			if event.Entry.EntryType == protocol.FileTypeRegularFile {
				fullPath := filepath.Join(engine.BaseDir(), event.Entry.Path)
				relPath := event.Entry.Path
				_ = utils.ReadFileChunked(fullPath, func(offset uint64, data []byte, isFinal bool) error {
					flags := byte(0)
					if isFinal {
						flags |= protocol.FlagFinalChunk
					}
					return c.Conn.WriteMessage(&protocol.Message{
						Type: protocol.MSG_FILE_DATA,
						FileData: &protocol.FileDataChunk{
							Path:   relPath,
							Flags:  flags,
							Offset: offset,
							Data:   data,
						},
					})
				})
			}
		}
	} else if event.Type == "delete" {
		engine.ScanLocal()
		if event.Path != "" {
			engine.RemoteTree().Remove(event.Path)

			clients := srv.GetClients()
			if len(clients) == 0 {
				// No clients connected — persist tombstones so they survive server restart
				if err := engine.SaveTombstones(); err != nil {
					fmt.Printf("save tombstones: %v\n", err)
				}
			} else {
				for _, c := range clients {
					msg := &protocol.Message{
						Type:       protocol.MSG_FILE_DELETE,
						FileDelete: event.Path,
					}
					_ = c.Conn.WriteMessage(msg)
				}
			}
		}
	}
}
