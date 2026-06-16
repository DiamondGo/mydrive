package sync

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	gosync "sync"

	"file-sync/protocol"
	"file-sync/utils"
)

// TombstoneFile is the hidden file name used to persist tombstones in the sync directory.
const TombstoneFile = ".sync-tombstones"

// Engine coordinates the sync process between local and remote sides.
// All methods are safe for concurrent use.
type Engine struct {
	mu         gosync.RWMutex
	baseDir    string
	localTree  *FileTree
	remoteTree *FileTree
	// tombstones tracks paths that were previously known and then deleted.
	// Used during initial sync to distinguish "never seen" from "deleted".
	tombstones map[string]bool
	tombMu     gosync.RWMutex
	// previousPaths tracks all paths ever seen in the local tree.
	// When a path disappears from localTree, it becomes a tombstone.
	previousPaths map[string]bool
}

// NewEngine creates a new sync engine for the given base directory.
// It loads any persisted tombstones from disk.
func NewEngine(baseDir string) *Engine {
	cleanDir := filepath.Clean(baseDir)
	e := &Engine{
		baseDir:       cleanDir,
		localTree:     NewFileTree(cleanDir),
		remoteTree:    NewFileTree(cleanDir),
		tombstones:    make(map[string]bool),
		previousPaths: make(map[string]bool),
	}
	// Load persisted tombstones
	e.loadTombstones()
	return e
}

// ScanLocal rescans the local directory and updates the local tree.
// Also updates tombstones: paths that were previously known but no longer exist.
func (e *Engine) ScanLocal() error {
	entries, err := utils.WalkDirectory(e.baseDir)
	if err != nil {
		return fmt.Errorf("walk directory: %w", err)
	}

	e.mu.Lock()
	newTree := NewFileTree(e.baseDir)
	currentPaths := make(map[string]bool, len(entries))
	for _, entry := range entries {
		// Skip the tombstone file itself
		if entry.Path == TombstoneFile {
			continue
		}
		newTree.Set(entry)
		currentPaths[entry.Path] = true
	}

	// Detect deletions: paths in previousPaths but not in currentPaths
	e.tombMu.Lock()
	for path := range e.previousPaths {
		if !currentPaths[path] {
			e.tombstones[path] = true
		}
	}
	// Update previousPaths to current
	e.previousPaths = currentPaths
	e.tombMu.Unlock()

	e.localTree = newTree
	e.mu.Unlock()
	return nil
}

// SetRemoteTree sets the remote tree from a received SyncTree message.
func (e *Engine) SetRemoteTree(tree *protocol.SyncTree) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.remoteTree = NewFileTree(e.baseDir)
	for _, entry := range tree.Entries {
		e.remoteTree.Set(entry)
	}
}

// ComputeDiff computes changes needed to make the local state match the remote state.
func (e *Engine) ComputeDiff() []*protocol.ChangeEvent {
	e.mu.RLock()
	local := e.remoteTree
	remote := e.localTree
	e.mu.RUnlock()
	return Diff(local, remote)
}

// ApplyChanges applies a list of SyncOps to the local filesystem.
func (e *Engine) ApplyChanges(ops []*protocol.SyncOp) error {
	for _, op := range ops {
		if err := e.applyOp(op); err != nil {
			return fmt.Errorf("apply op %s: %w", op.Type, err)
		}
	}
	// Rescan after applying
	return e.ScanLocal()
}

func (e *Engine) applyOp(op *protocol.SyncOp) error {
	switch op.Type {
	case "createDir":
		path := filepath.Join(e.baseDir, op.Entry.Path)
		return os.MkdirAll(path, os.FileMode(op.Entry.Mode))

	case "createFile":
		path := filepath.Join(e.baseDir, op.Entry.Path)
		if err := utils.EnsureDir(path); err != nil {
			return err
		}
		f, err := os.Create(path)
		if err != nil {
			return err
		}
		f.Close()
		return utils.ApplyAttrs(path, op.Entry)

	case "writeFile":
		path := filepath.Join(e.baseDir, op.Path)
		if err := utils.EnsureDir(path); err != nil {
			return err
		}
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = f.WriteAt(op.Chunk, int64(op.ChunkOff))
		return err

	case "setAttrs":
		path := filepath.Join(e.baseDir, op.Path)
		return utils.ApplyAttrs(path, op.Entry)

	case "delete":
		path := filepath.Join(e.baseDir, op.Path)
		return utils.DeleteEntry(path)

	case "move":
		oldPath := filepath.Join(e.baseDir, op.OldPath)
		newPath := filepath.Join(e.baseDir, op.NewPath)
		if err := utils.EnsureDir(newPath); err != nil {
			return err
		}
		return os.Rename(oldPath, newPath)

	default:
		return fmt.Errorf("unknown op type: %s", op.Type)
	}
}

// BuildSyncTree serializes the local tree into a SyncTree message payload.
func (e *Engine) BuildSyncTree() *protocol.SyncTree {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return &protocol.SyncTree{
		Entries: e.localTree.Entries(),
	}
}

// LocalTree returns the local file tree.
func (e *Engine) LocalTree() *FileTree {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.localTree
}

// RemoteTree returns the remote file tree.
func (e *Engine) RemoteTree() *FileTree {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.remoteTree
}

// BaseDir returns the base directory.
func (e *Engine) BaseDir() string {
	return e.baseDir
}

// IsTombstoned returns true if the given path was previously known and then deleted.
func (e *Engine) IsTombstoned(path string) bool {
	e.tombMu.RLock()
	defer e.tombMu.RUnlock()
	return e.tombstones[path]
}

// Tombstones returns a copy of all tombstoned paths.
func (e *Engine) Tombstones() map[string]bool {
	e.tombMu.RLock()
	defer e.tombMu.RUnlock()
	result := make(map[string]bool, len(e.tombstones))
	for k, v := range e.tombstones {
		result[k] = v
	}
	return result
}

// ClearTombstones removes all tombstones (called after successful sync).
// Also removes the persisted tombstone file from disk.
func (e *Engine) ClearTombstones() {
	e.tombMu.Lock()
	defer e.tombMu.Unlock()
	e.tombstones = make(map[string]bool)
	// Remove the persisted file
	os.Remove(filepath.Join(e.baseDir, TombstoneFile))
}

// ClearTombstone removes a single path from the tombstone set.
func (e *Engine) ClearTombstone(path string) {
	e.tombMu.Lock()
	defer e.tombMu.Unlock()
	delete(e.tombstones, path)
}

// AddToPreviousPaths adds paths to the set of previously known paths.
// Used when receiving a remote tree to track what was known.
func (e *Engine) AddToPreviousPaths(paths []string) {
	e.tombMu.Lock()
	defer e.tombMu.Unlock()
	for _, p := range paths {
		e.previousPaths[p] = true
	}
}

// SaveTombstones persists the current tombstones and known paths to disk.
// Called when a deletion happens but can't be synced immediately
// (e.g., no clients connected), or during graceful shutdown.
func (e *Engine) SaveTombstones() error {
	e.tombMu.RLock()
	tombstones := make(map[string]bool, len(e.tombstones))
	for k, v := range e.tombstones {
		tombstones[k] = v
	}
	previousPaths := make(map[string]bool, len(e.previousPaths))
	for k, v := range e.previousPaths {
		previousPaths[k] = v
	}
	e.tombMu.RUnlock()

	if len(tombstones) == 0 && len(previousPaths) == 0 {
		os.Remove(filepath.Join(e.baseDir, TombstoneFile))
		return nil
	}

	path := filepath.Join(e.baseDir, TombstoneFile)
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create tombstone file: %w", err)
	}
	defer f.Close()

	// Write tombstoned paths (deleted files)
	for p := range tombstones {
		fmt.Fprintf(f, "D:%s\n", p)
	}
	// Write known paths (for detecting deletions across restarts)
	for p := range previousPaths {
		fmt.Fprintf(f, "K:%s\n", p)
	}
	return nil
}

// loadTombstones reads persisted tombstones and known paths from disk.
func (e *Engine) loadTombstones() {
	path := filepath.Join(e.baseDir, TombstoneFile)
	f, err := os.Open(path)
	if err != nil {
		return // File doesn't exist or can't be read
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "D:") {
			p := line[2:]
			e.tombstones[p] = true
			e.previousPaths[p] = true
		} else if strings.HasPrefix(line, "K:") {
			p := line[2:]
			e.previousPaths[p] = true
		} else {
			// Legacy format (no prefix) — treat as tombstone
			e.tombstones[line] = true
			e.previousPaths[line] = true
		}
	}
}

// HasTombstones returns true if there are any tombstones.
func (e *Engine) HasTombstones() bool {
	e.tombMu.RLock()
	defer e.tombMu.RUnlock()
	return len(e.tombstones) > 0
}
