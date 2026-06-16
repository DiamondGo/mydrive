package netlayer

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"file-sync/protocol"
	"file-sync/utils"
)

// Client is a TCP client that connects to a sync server.
type Client struct {
	addr      string
	conn      *Conn
	mu        sync.Mutex // protects conn
	reconnect bool
	maxDelay  time.Duration
}

// NewClient creates a new TCP client.
func NewClient(addr string) *Client {
	return &Client{
		addr:      addr,
		reconnect: true,
		maxDelay:  60 * time.Second,
	}
}

// Connect establishes a connection to the server with exponential backoff.
// If a previous connection exists, it is closed first.
// The context can be used to cancel the connection attempt.
func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
	c.mu.Unlock()

	delay := 1 * time.Second
	for {
		// Check context before attempting connection
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		conn, err := net.DialTimeout("tcp", c.addr, 10*time.Second)
		if err != nil {
			if c.reconnect {
				fmt.Printf("connect to %s: %v, retrying in %v...\n", c.addr, err, delay)
				// Wait with context awareness
				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return ctx.Err()
				}
				// Exponential backoff with cap
				delay = delay * 2
				if delay > c.maxDelay {
					delay = c.maxDelay
				}
				continue
			}
			return fmt.Errorf("connect: %w", err)
		}

		c.mu.Lock()
		c.conn = NewConn(conn)
		c.mu.Unlock()
		fmt.Printf("connected to %s\n", c.addr)
		return nil
	}
}

// SendHello sends a HELLO message and reads the HELLO_ACK response.
func (c *Client) SendHello(clientID, syncDir, version string) (*protocol.HelloAckPayload, error) {
	msg := &protocol.Message{
		Type: protocol.MSG_HELLO,
		Hello: &protocol.HelloPayload{
			ClientID: clientID,
			SyncDir:  syncDir,
			Version:  version,
		},
	}

	if err := c.conn.WriteMessage(msg); err != nil {
		return nil, fmt.Errorf("send hello: %w", err)
	}

	resp, err := c.conn.ReadMessage()
	if err != nil {
		return nil, fmt.Errorf("read hello ack: %w", err)
	}

	if resp.Type != protocol.MSG_HELLO_ACK {
		return nil, fmt.Errorf("expected HELLO_ACK, got 0x%02x", resp.Type)
	}

	if resp.HelloAck.Status != 0 {
		return nil, fmt.Errorf("hello ack status: %d", resp.HelloAck.Status)
	}

	return resp.HelloAck, nil
}

// Send writes a message to the server.
func (c *Client) Send(msg *protocol.Message) error {
	return c.conn.WriteMessage(msg)
}

// Receive reads a message from the server.
func (c *Client) Receive() (*protocol.Message, error) {
	return c.conn.ReadMessage()
}

// Close closes the connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		return err
	}
	return nil
}

// IsConnected returns true if the client has an active connection.
func (c *Client) IsConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn != nil
}

// SetReadDeadline sets the read deadline.
func (c *Client) SetReadDeadline(t time.Time) error {
	if c.conn == nil {
		return nil
	}
	return c.conn.SetReadDeadline(t)
}

// SendPing sends a keep-alive ping.
func (c *Client) SendPing() error {
	msg := &protocol.Message{Type: protocol.MSG_PING}
	return c.conn.WriteMessage(msg)
}

// SendPong sends a pong response to a ping.
func (c *Client) SendPong() error {
	msg := &protocol.Message{Type: protocol.MSG_PONG}
	return c.conn.WriteMessage(msg)
}

// SendAck sends an ACK for the given sequence number.
func (c *Client) SendAck(seq uint64, status byte) error {
	msg := &protocol.Message{
		Type: protocol.MSG_ACK,
		Ack: &protocol.AckPayload{
			Seq:    seq,
			Status: status,
		},
	}
	return c.conn.WriteMessage(msg)
}

// SendSyncInit sends the local file tree to the server.
func (c *Client) SendSyncInit(tree *protocol.SyncTree) error {
	msg := &protocol.Message{
		Type:     protocol.MSG_SYNC_INIT,
		SyncInit: tree,
	}
	return c.conn.WriteMessage(msg)
}

// SendFileCreate notifies the server of a new file.
func (c *Client) SendFileCreate(entry *protocol.FileEntry) error {
	msg := &protocol.Message{
		Type:       protocol.MSG_FILE_CREATE,
		FileCreate: entry,
	}
	return c.conn.WriteMessage(msg)
}

// SendFileModify notifies the server of a modified file.
func (c *Client) SendFileModify(entry *protocol.FileEntry) error {
	msg := &protocol.Message{
		Type:       protocol.MSG_FILE_MODIFY,
		FileModify: entry,
	}
	return c.conn.WriteMessage(msg)
}

// SendFileDelete notifies the server of a deleted file.
func (c *Client) SendFileDelete(path string) error {
	msg := &protocol.Message{
		Type:       protocol.MSG_FILE_DELETE,
		FileDelete: path,
	}
	return c.conn.WriteMessage(msg)
}

// SendFileData sends a file data chunk to the server.
func (c *Client) SendFileData(chunk *protocol.FileDataChunk) error {
	msg := &protocol.Message{
		Type:     protocol.MSG_FILE_DATA,
		FileData: chunk,
	}
	return c.conn.WriteMessage(msg)
}

// SendFileContent streams the content of a file to the server in chunks.
func (c *Client) SendFileContent(fullPath, relPath string) error {
	return utils.ReadFileChunked(fullPath, func(offset uint64, data []byte, isFinal bool) error {
		flags := byte(0)
		if isFinal {
			flags |= protocol.FlagFinalChunk
		}
		chunk := &protocol.FileDataChunk{
			Path:   relPath,
			Flags:  flags,
			Offset: offset,
			Data:   data,
		}
		return c.SendFileData(chunk)
	})
}
