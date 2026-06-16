package config

// ServerConfig holds server-side configuration.
type ServerConfig struct {
	ListenAddr  string `json:"listenAddr" toml:"listenAddr"`
	ServerDir   string `json:"serverDir" toml:"serverDir"`
	LogLevel    string `json:"logLevel" toml:"logLevel"`
	MaxClients  int    `json:"maxClients" toml:"maxClients"`
}

// ClientConfig holds client-side configuration.
type ClientConfig struct {
	ServerAddr  string `json:"serverAddr" toml:"serverAddr"`
	LocalDir    string `json:"localDir" toml:"localDir"`
	ClientID    string `json:"clientId" toml:"clientId"`
	LogLevel    string `json:"logLevel" toml:"logLevel"`
	Reconnect   bool   `json:"reconnect" toml:"reconnect"`
	MaxDelaySec int    `json:"maxDelaySec" toml:"maxDelaySec"`
}

// DefaultServerConfig returns a server config with sensible defaults.
func DefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		ListenAddr: ":8765",
		ServerDir:  "/data/sync",
		LogLevel:   "info",
		MaxClients: 50,
	}
}

// DefaultClientConfig returns a client config with sensible defaults.
func DefaultClientConfig() *ClientConfig {
	return &ClientConfig{
		ServerAddr:  "127.0.0.1:8765",
		LocalDir:    "/home/user/sync",
		ClientID:    "",
		LogLevel:    "info",
		Reconnect:   true,
		MaxDelaySec: 60,
	}
}
