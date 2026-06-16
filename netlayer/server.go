package netlayer

import (
	"errors"
	"file-sync/protocol"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

// Server is a TCP server that accepts client connections.
type Server struct {
	addr     string
	mu       sync.Mutex
	clients  map[uint32]*ClientSession
	nextID   uint32
	listener net.Listener
}

// ClientSession represents a connected client.
type ClientSession struct {
	ID      uint32
	Conn    *Conn
	SyncDir string
}

// NewServer creates a new TCP server.
func NewServer(addr string) *Server {
	return &Server{
		addr:    addr,
		clients: make(map[uint32]*ClientSession),
		nextID:  1, // Start at 1 to avoid confusion with zero-value uint32
	}
}

// ListenAndServe starts accepting connections.
// The handler function is called for each new connection.
func (s *Server) ListenAndServe(handler func(session *ClientSession)) error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()

	defer listener.Close()

	fmt.Printf("server listening on %s\n", s.addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			// If the listener was closed (graceful shutdown), return
			if errors.Is(err, net.ErrClosed) || strings.Contains(err.Error(), "use of closed network connection") {
				return fmt.Errorf("accept: %w", err)
			}
			// Transient error — log and continue accepting
			fmt.Printf("accept error (transient, retrying): %v\n", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		s.mu.Lock()
		id := s.nextID
		s.nextID++
		session := &ClientSession{
			ID:   id,
			Conn: NewConn(conn),
		}
		s.clients[id] = session
		s.mu.Unlock()

		fmt.Printf("client connected: id=%d addr=%s\n", id, conn.RemoteAddr())

		go func(session *ClientSession) {
			defer func() {
				session.Conn.Close()
				s.mu.Lock()
				delete(s.clients, session.ID)
				s.mu.Unlock()
				fmt.Printf("client disconnected: id=%d\n", session.ID)
			}()
			handler(session)
		}(session)
	}
}

// Close closes the listener, causing ListenAndServe to return.
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

// GetClients returns a copy of all client sessions.
func (s *Server) GetClients() []*ClientSession {
	s.mu.Lock()
	defer s.mu.Unlock()

	result := make([]*ClientSession, 0, len(s.clients))
	for _, c := range s.clients {
		result = append(result, c)
	}
	return result
}

// GetClient returns a client session by ID.
func (s *Server) GetClient(id uint32) *ClientSession {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.clients[id]
}

// Broadcast sends a message to all connected clients except the sender.
func (s *Server) Broadcast(senderID uint32, msg *protocol.Message) {
	s.mu.Lock()
	clients := make([]*ClientSession, 0, len(s.clients))
	for _, c := range s.clients {
		if c.ID != senderID {
			clients = append(clients, c)
		}
	}
	s.mu.Unlock()

	for _, c := range clients {
		if err := c.Conn.WriteMessage(msg); err != nil {
			fmt.Printf("broadcast to client %d: %v\n", c.ID, err)
		}
	}
}
