package protocol

// Message type IDs
const (
	MSG_HELLO        byte = 0x01
	MSG_HELLO_ACK    byte = 0x02
	MSG_SYNC_INIT    byte = 0x03
	MSG_SYNC_INIT_ACK byte = 0x04
	MSG_FILE_CREATE  byte = 0x05
	MSG_FILE_DATA    byte = 0x06
	MSG_FILE_MODIFY  byte = 0x07
	MSG_FILE_DELETE  byte = 0x08
	MSG_FILE_MOVE    byte = 0x09
	MSG_ACK          byte = 0x0A
	MSG_SYNC_DIFF    byte = 0x0B
	MSG_SYNC_APPLY   byte = 0x0C
	MSG_PING         byte = 0x0D
	MSG_PONG         byte = 0x0E
	MSG_ERROR        byte = 0x0F
)

// File type constants
const (
	FileTypeRegularFile byte = 0x01
	FileTypeDirectory   byte = 0x02
	FileTypeSymlink     byte = 0x04
	FileTypeOther       byte = 0x06
)

// Flag for file data chunks
const FlagFinalChunk byte = 0x01

// FileEntry represents metadata for a single file or directory.
type FileEntry struct {
	Path         string `json:"path"`
	EntryType    byte   `json:"entryType"`
	Mode         uint32 `json:"mode"`
	UID          uint32 `json:"uid"`
	GID          uint32 `json:"gid"`
	MTime        int64  `json:"mtime"` // nanoseconds since epoch
	ATime        int64  `json:"atime"`
	Size         uint64 `json:"size"`
	ContentHash  [32]byte `json:"contentHash"`
	Seq          uint64 `json:"seq"`
}

// FileDataChunk represents a chunk of file content for streaming transfers.
type FileDataChunk struct {
	Path   string `json:"path"`   // Relative path of the file being transferred
	Flags  byte   `json:"flags"`
	Offset uint64 `json:"offset"`
	Data   []byte `json:"data"`
}

// ChangeEvent represents a filesystem change.
type ChangeEvent struct {
	Type     string      `json:"type"` // "create", "modify", "delete", "move"
	Entry    *FileEntry  `json:"entry,omitempty"`
	Path     string      `json:"path,omitempty"`
	OldPath  string      `json:"oldPath,omitempty"`
	NewPath  string      `json:"newPath,omitempty"`
}

// SyncOp represents an operation to apply during sync.
type SyncOp struct {
	Type     string       `json:"type"` // "createDir", "createFile", "writeFile", "setAttrs", "delete", "move"
	Entry    *FileEntry   `json:"entry,omitempty"`
	Path     string       `json:"path,omitempty"`
	OldPath  string       `json:"oldPath,omitempty"`
	NewPath  string       `json:"newPath,omitempty"`
	Chunk    []byte       `json:"chunk,omitempty"`
	ChunkOff uint64       `json:"chunkOff,omitempty"`
	ChunkLen uint64       `json:"chunkLen,omitempty"`
	ChunkFlags byte       `json:"chunkFlags,omitempty"`
}

// HelloPayload is sent by the client during handshake.
type HelloPayload struct {
	ClientID string `json:"clientId"`
	SyncDir  string `json:"syncDir"`
	Version  string `json:"version"`
}

// HelloAckPayload is sent by the server in response to HELLO.
type HelloAckPayload struct {
	ServerID string `json:"serverId"`
	ClientID uint32 `json:"clientId"`
	Status   byte   `json:"status"` // 0 = OK
}

// AckPayload acknowledges an operation.
type AckPayload struct {
	Seq    uint64 `json:"seq"`
	Status byte   `json:"status"` // 0 = OK
}

// SyncTree is a batch of file entries for initial sync.
type SyncTree struct {
	Entries []*FileEntry `json:"entries"`
}

// SyncDiff is a list of computed changes.
type SyncDiff struct {
	Changes []*ChangeEvent `json:"changes"`
}

// SyncApply is a list of operations to execute.
type SyncApply struct {
	Ops []*SyncOp `json:"ops"`
}

// ErrorPayload carries an error notification.
type ErrorPayload struct {
	Code    uint32 `json:"code"`
	Message string `json:"message"`
}

// Message is the top-level protocol message union.
type Message struct {
	Type byte
	// One of the following is non-nil based on Type
	Hello        *HelloPayload
	HelloAck     *HelloAckPayload
	SyncInit     *SyncTree
	SyncInitAck  *SyncTree
	FileCreate   *FileEntry
	FileData     *FileDataChunk
	FileModify   *FileEntry
	FileDelete   string
	FileMove     *FileMoveData
	Ack          *AckPayload
	SyncDiff     *SyncDiff
	SyncApply    *SyncApply
	Error        *ErrorPayload
}

type FileMoveData struct {
	OldPath string     `json:"oldPath"`
	NewPath string     `json:"newPath"`
	Entry   *FileEntry `json:"entry"`
}
