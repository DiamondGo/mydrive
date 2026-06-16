package netlayer

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"file-sync/protocol"
)

// Conn wraps a net.Conn with protocol-level send/receive methods.
type Conn struct {
	conn net.Conn
	r    *bufio.Reader
	wmu  sync.Mutex // protects concurrent writes
}

// NewConn wraps a net.Conn for protocol messaging.
func NewConn(c net.Conn) *Conn {
	return &Conn{
		conn: c,
		r:    bufio.NewReader(c),
	}
}

// ReadMessage reads a single frame from the connection.
func (c *Conn) ReadMessage() (*protocol.Message, error) {
	header := make([]byte, protocol.HeaderSize)
	if _, err := io.ReadFull(c.r, header); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	msgType := header[0]
	length := binary.BigEndian.Uint32(header[1:5])

	// Validate frame size to prevent excessive memory allocation
	if length > protocol.MaxFrameSize {
		return nil, fmt.Errorf("%w: %d bytes", protocol.ErrFrameTooLarge, length)
	}

	if length == 0 && msgType != protocol.MSG_PING && msgType != protocol.MSG_PONG {
		return nil, fmt.Errorf("empty payload for type 0x%02x", msgType)
	}

	if length > 0 {
		payload := make([]byte, length)
		if _, err := io.ReadFull(c.r, payload); err != nil {
			return nil, fmt.Errorf("read payload: %w", err)
		}
		return protocol.DecodeFromBytes(msgType, payload)
	}

	return &protocol.Message{Type: msgType}, nil
}

// WriteMessage sends a single message frame. It is safe for concurrent use.
func (c *Conn) WriteMessage(msg *protocol.Message) error {
	data, err := protocol.Encode(msg)
	if err != nil {
		return fmt.Errorf("encode message: %w", err)
	}

	c.wmu.Lock()
	_, err = c.conn.Write(data)
	c.wmu.Unlock()
	return err
}

// Close closes the underlying connection.
func (c *Conn) Close() error {
	return c.conn.Close()
}

// RemoteAddr returns the remote address.
func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// SetReadDeadline sets the read deadline.
func (c *Conn) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

// SetWriteDeadline sets the write deadline.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}
