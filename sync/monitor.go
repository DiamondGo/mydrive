package sync

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"file-sync/protocol"
	"file-sync/utils"

	"sync"

	"github.com/fsnotify/fsnotify"
)

// Monitor watches a directory for filesystem changes using fsnotify.
type Monitor struct {
	watcher   *fsnotify.Watcher
	baseDir   string
	ch        chan *protocol.ChangeEvent
	quit      chan struct{}
	running   bool
	mu        sync.Mutex
	debounce  map[string]*debounceState
	ignoreMap map[string]time.Time
	ignoreMu  sync.Mutex
}

type debounceState struct {
	timer  *time.Timer
	lastOp fsnotify.Op
}

// NewMonitor creates a new filesystem monitor for the given directory.
func NewMonitor(baseDir string) (*Monitor, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("create watcher: %w", err)
	}

	absDir, err := filepath.Abs(baseDir)
	if err != nil {
		watcher.Close()
		return nil, fmt.Errorf("resolve path: %w", err)
	}

	m := &Monitor{
		watcher:   watcher,
		baseDir:   absDir,
		ch:        make(chan *protocol.ChangeEvent, 256),
		quit:      make(chan struct{}),
		ignoreMap: make(map[string]time.Time),
	}

	if err := m.addWatch(absDir); err != nil {
		watcher.Close()
		return nil, fmt.Errorf("add initial watch: %w", err)
	}

	return m, nil
}

func (m *Monitor) addWatch(dir string) error {
	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if err := m.watcher.Add(path); err != nil {
				return fmt.Errorf("watch %s: %w", path, err)
			}
		}
		return nil
	})
}

// Events returns the channel receiving change events.
func (m *Monitor) Events() <-chan *protocol.ChangeEvent {
	return m.ch
}

// Start begins watching for filesystem events.
func (m *Monitor) Start() {
	m.running = true
	go m.loop()
}

// Stop shuts down the monitor.
func (m *Monitor) Stop() {
	if !m.running {
		return
	}
	m.running = false
	close(m.quit)
	m.watcher.Close()
}

func (m *Monitor) loop() {
	m.mu.Lock()
	m.debounce = make(map[string]*debounceState)
	m.mu.Unlock()
	debounceDelay := 200 * time.Millisecond

	defer func() {
		close(m.ch)
		m.mu.Lock()
		for _, d := range m.debounce {
			d.timer.Stop()
		}
		m.mu.Unlock()
	}()

	for {
		select {
		case <-m.quit:
			return

		case event, ok := <-m.watcher.Events:
			if !ok {
				return
			}
			relPath, _ := filepath.Rel(m.baseDir, event.Name)
			if relPath == "" {
				relPath = "."
			}

			// Skip the tombstone file
			if relPath == TombstoneFile {
				continue
			}

			if m.isIgnored(relPath) {
				continue
			}

			// Handle directory create: add watch
			if event.Op&fsnotify.Create != 0 {
				if info, err := os.Stat(event.Name); err == nil && info.IsDir() {
					m.watcher.Add(event.Name)
				}
			}

			m.mu.Lock()
			// Debounce: coalesce events for same path
			if state, ok := m.debounce[relPath]; ok {
				state.lastOp |= event.Op
				state.timer.Reset(debounceDelay)
				m.mu.Unlock()
				continue
			}

			state := &debounceState{
				timer:  time.AfterFunc(debounceDelay, func() { m.flushDebounce(relPath) }),
				lastOp: event.Op,
			}
			m.debounce[relPath] = state
			m.mu.Unlock()

		case err := <-m.watcher.Errors:
			fmt.Printf("monitor error: %v\n", err)
		}
	}
}

func (m *Monitor) Ignore(path string, duration time.Duration) {
	m.ignoreMu.Lock()
	defer m.ignoreMu.Unlock()
	m.ignoreMap[path] = time.Now().Add(duration)
}

func (m *Monitor) isIgnored(path string) bool {
	m.ignoreMu.Lock()
	defer m.ignoreMu.Unlock()
	until, ok := m.ignoreMap[path]
	if !ok {
		return false
	}
	if time.Now().After(until) {
		delete(m.ignoreMap, path)
		return false
	}
	return true
}

func (m *Monitor) flushDebounce(relPath string) {
	m.mu.Lock()
	state := m.debounce[relPath]
	if state == nil {
		m.mu.Unlock()
		return
	}

	// Determine dominant event type
	var eventType string
	switch {
	case state.lastOp&fsnotify.Remove != 0 || state.lastOp&fsnotify.Rename != 0:
		eventType = "delete"
	case state.lastOp&fsnotify.Create != 0:
		eventType = "create"
	case state.lastOp&fsnotify.Write != 0:
		eventType = "modify"
	default:
		m.mu.Unlock()
		return
	}

	ce := &protocol.ChangeEvent{
		Type: eventType,
		Path: relPath,
	}

	if eventType == "create" || eventType == "modify" {
		fullPath := filepath.Join(m.baseDir, relPath)
		if entry, err := utils.BuildEntry(m.baseDir, fullPath, 0); err == nil {
			ce.Entry = entry
		}
	}

	delete(m.debounce, relPath)
	m.mu.Unlock()

	select {
	case m.ch <- ce:
	default:
		// Drop if channel full
	}
}
