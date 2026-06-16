package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	// Header: 1 byte type + 4 bytes length
	HeaderSize = 5

	// Max payload size: 16 MB
	MaxFrameSize = 16 * 1024 * 1024
)

var (
	ErrFrameTooLarge  = errors.New("frame too large")
	ErrIncompleteFrame = errors.New("incomplete frame")
	ErrInvalidType    = errors.New("invalid message type")
)

// Encode serializes a Message into a length-prefixed binary frame.
// Wire format: [type:1][length:4 BE][payload:JSON]
func Encode(msg *Message) ([]byte, error) {
	// PING/PONG have no payload — send empty frame
	if msg.Type == MSG_PING || msg.Type == MSG_PONG {
		buf := make([]byte, HeaderSize)
		buf[0] = msg.Type
		binary.BigEndian.PutUint32(buf[1:5], 0)
		return buf, nil
	}

	payload, err := jsonMarshal(msg.payload())
	if err != nil {
		return nil, fmt.Errorf("encode payload: %w", err)
	}
	if len(payload) > MaxFrameSize {
		return nil, fmt.Errorf("%w: %d bytes", ErrFrameTooLarge, len(payload))
	}

	buf := make([]byte, HeaderSize+len(payload))
	buf[0] = msg.Type
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(payload)))
	copy(buf[HeaderSize:], payload)
	return buf, nil
}

// DecodeFrame reads a single frame from r and parses it into a Message.
func DecodeFrame(r io.Reader) (*Message, error) {
	// Read header
	header := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	msgType := header[0]
	length := binary.BigEndian.Uint32(header[1:5])
	if length > MaxFrameSize {
		return nil, fmt.Errorf("%w: %d bytes", ErrFrameTooLarge, length)
	}

	if length == 0 && msgType != MSG_PING && msgType != MSG_PONG {
		return nil, fmt.Errorf("empty payload for type 0x%02x", msgType)
	}

	if length > 0 {
		payload := make([]byte, length)
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, fmt.Errorf("read payload: %w", err)
		}
		return parseMessage(msgType, payload)
	}

	// Ping/Pong have no payload
	return &Message{Type: msgType}, nil
}

// DecodeFromBytes parses a message from a pre-read payload.
func DecodeFromBytes(t byte, payload []byte) (*Message, error) {
	if len(payload) == 0 && t != MSG_PING && t != MSG_PONG {
		return nil, fmt.Errorf("empty payload for type 0x%02x", t)
	}
	if len(payload) == 0 {
		return &Message{Type: t}, nil
	}
	return parseMessage(t, payload)
}

func parseMessage(t byte, payload []byte) (*Message, error) {
	msg := &Message{Type: t}
	switch t {
	case MSG_HELLO:
		p := &HelloPayload{}
		if err := jsonUnmarshal(payload, p); err != nil {
			return nil, err
		}
		msg.Hello = p
	case MSG_HELLO_ACK:
		p := &HelloAckPayload{}
		if err := jsonUnmarshal(payload, p); err != nil {
			return nil, err
		}
		msg.HelloAck = p
	case MSG_SYNC_INIT:
		p := &SyncTree{}
		if err := jsonUnmarshal(payload, p); err != nil {
			return nil, err
		}
		msg.SyncInit = p
	case MSG_SYNC_INIT_ACK:
		p := &SyncTree{}
		if err := jsonUnmarshal(payload, p); err != nil {
			return nil, err
		}
		msg.SyncInitAck = p
	case MSG_FILE_CREATE:
		p := &FileEntry{}
		if err := jsonUnmarshal(payload, p); err != nil {
			return nil, err
		}
		msg.FileCreate = p
	case MSG_FILE_DATA:
		p := &FileDataChunk{}
		if err := jsonUnmarshal(payload, p); err != nil {
			return nil, err
		}
		msg.FileData = p
	case MSG_FILE_MODIFY:
		p := &FileEntry{}
		if err := jsonUnmarshal(payload, p); err != nil {
			return nil, err
		}
		msg.FileModify = p
	case MSG_FILE_DELETE:
		var path string
		if err := jsonUnmarshal(payload, &path); err != nil {
			return nil, err
		}
		msg.FileDelete = path
	case MSG_FILE_MOVE:
		p := &FileMoveData{}
		if err := jsonUnmarshal(payload, p); err != nil {
			return nil, err
		}
		msg.FileMove = p
	case MSG_ACK:
		p := &AckPayload{}
		if err := jsonUnmarshal(payload, p); err != nil {
			return nil, err
		}
		msg.Ack = p
	case MSG_SYNC_DIFF:
		p := &SyncDiff{}
		if err := jsonUnmarshal(payload, p); err != nil {
			return nil, err
		}
		msg.SyncDiff = p
	case MSG_SYNC_APPLY:
		p := &SyncApply{}
		if err := jsonUnmarshal(payload, p); err != nil {
			return nil, err
		}
		msg.SyncApply = p
	case MSG_ERROR:
		p := &ErrorPayload{}
		if err := jsonUnmarshal(payload, p); err != nil {
			return nil, err
		}
		msg.Error = p
	case MSG_PING, MSG_PONG:
		// no payload
	default:
		return nil, fmt.Errorf("%w: 0x%02x", ErrInvalidType, t)
	}
	return msg, nil
}

func (m *Message) payload() interface{} {
	switch m.Type {
	case MSG_HELLO:
		return m.Hello
	case MSG_HELLO_ACK:
		return m.HelloAck
	case MSG_SYNC_INIT:
		return m.SyncInit
	case MSG_SYNC_INIT_ACK:
		return m.SyncInitAck
	case MSG_FILE_CREATE:
		return m.FileCreate
	case MSG_FILE_DATA:
		return m.FileData
	case MSG_FILE_MODIFY:
		return m.FileModify
	case MSG_FILE_DELETE:
		return m.FileDelete
	case MSG_FILE_MOVE:
		return m.FileMove
	case MSG_ACK:
		return m.Ack
	case MSG_SYNC_DIFF:
		return m.SyncDiff
	case MSG_SYNC_APPLY:
		return m.SyncApply
	case MSG_ERROR:
		return m.Error
	case MSG_PING, MSG_PONG:
		return nil
	default:
		return nil
	}
}
