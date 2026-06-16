package sync

import (
	"bytes"
	"file-sync/protocol"
)

// Diff computes the differences between two FileTrees.
// Returns a list of ChangeEvents describing what changed from local to remote.
//
// Comparison strategy:
// - Files in local but not in remote -> Create
// - Files in remote but not in local -> Delete
// - Files in both but different hash or mtime -> Modify (last-write-wins)
func Diff(local, remote *FileTree) []*protocol.ChangeEvent {
	var changes []*protocol.ChangeEvent

	// Take snapshots to avoid holding locks during iteration
	localSnap := local.Snapshot()
	remoteSnap := remote.Snapshot()

	// Find new and modified files (present in local)
	for path, lEntry := range localSnap {
		rEntry := remoteSnap[path]
		if rEntry == nil {
			// New file in local
			changes = append(changes, &protocol.ChangeEvent{
				Type:  "create",
				Entry: lEntry,
			})
		} else if entryChanged(lEntry, rEntry) {
			// Modified: last-write-wins by mtime
			winner := pickWinner(lEntry, rEntry)
			changes = append(changes, &protocol.ChangeEvent{
				Type:  "modify",
				Entry: winner,
			})
		}
	}

	// Find deleted files (present in remote but not in local)
	for path := range remoteSnap {
		if _, ok := localSnap[path]; !ok {
			changes = append(changes, &protocol.ChangeEvent{
				Type: "delete",
				Path: path,
			})
		}
	}

	return changes
}

// DiffInitial computes differences for initial sync (no deletions).
// During initial sync, files present on one side but not the other should be
// created (merged), not deleted. Only create and modify events are returned.
func DiffInitial(local, remote *FileTree) []*protocol.ChangeEvent {
	var changes []*protocol.ChangeEvent

	localSnap := local.Snapshot()
	remoteSnap := remote.Snapshot()

	// Find new and modified files (present in local but not remote)
	for path, lEntry := range localSnap {
		rEntry := remoteSnap[path]
		if rEntry == nil {
			// New file in local — create on remote side
			changes = append(changes, &protocol.ChangeEvent{
				Type:  "create",
				Entry: lEntry,
			})
		} else if entryChanged(lEntry, rEntry) {
			winner := pickWinner(lEntry, rEntry)
			changes = append(changes, &protocol.ChangeEvent{
				Type:  "modify",
				Entry: winner,
			})
		}
	}

	// NOTE: No deletions during initial sync — files only on remote are kept

	return changes
}

// entryChanged returns true if two entries differ in content or metadata.
func entryChanged(a, b *protocol.FileEntry) bool {
	if a.EntryType != b.EntryType {
		return true
	}
	if a.MTime != b.MTime {
		return true
	}
	if a.EntryType == protocol.FileTypeRegularFile {
		if !bytes.Equal(a.ContentHash[:], b.ContentHash[:]) {
			return true
		}
	}
	if a.Mode != b.Mode {
		return true
	}
	return false
}

// pickWinner selects the entry with the later mtime (last-write-wins).
// On tie, remote wins.
func pickWinner(local, remote *protocol.FileEntry) *protocol.FileEntry {
	if remote.MTime >= local.MTime {
		return remote
	}
	return local
}
