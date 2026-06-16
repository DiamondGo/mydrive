package sync

import (
	"file-sync/protocol"
	gosync "sync"
)

// FileTree is an in-memory state store mapping file paths to their metadata.
type FileTree struct {
	mu      gosync.RWMutex
	entries map[string]*protocol.FileEntry
	nextSeq uint64
	baseDir string
}

// NewFileTree creates a new empty FileTree.
func NewFileTree(baseDir string) *FileTree {
	return &FileTree{
		entries: make(map[string]*protocol.FileEntry),
		baseDir: baseDir,
	}
}

// Get returns the entry for a path, or nil if not found.
func (t *FileTree) Get(path string) *protocol.FileEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.entries[path]
}

// Set adds or updates an entry.
func (t *FileTree) Set(entry *protocol.FileEntry) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if entry.Seq == 0 {
		entry.Seq = t.nextSeq
		t.nextSeq++
	}
	t.entries[entry.Path] = entry
}

// Remove deletes an entry by path.
func (t *FileTree) Remove(path string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.entries, path)
}

// Entries returns all entries as a slice (snapshot).
func (t *FileTree) Entries() []*protocol.FileEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()
	result := make([]*protocol.FileEntry, 0, len(t.entries))
	for _, e := range t.entries {
		result = append(result, e)
	}
	return result
}

// Len returns the number of entries.
func (t *FileTree) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.entries)
}

// Contains returns true if the path exists in the tree.
func (t *FileTree) Contains(path string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.entries[path]
	return ok
}

// Snapshot returns a shallow copy of the internal entries map for iteration
// without holding the lock. Used by Diff to avoid holding the lock during
// potentially long operations.
func (t *FileTree) Snapshot() map[string]*protocol.FileEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()
	snap := make(map[string]*protocol.FileEntry, len(t.entries))
	for k, v := range t.entries {
		snap[k] = v
	}
	return snap
}

// Paths returns all paths in the tree.
func (t *FileTree) Paths() map[string]bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	paths := make(map[string]bool, len(t.entries))
	for k := range t.entries {
		paths[k] = true
	}
	return paths
}
