package app

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"clickhouse-mcp/clickhouse"
	"clickhouse-mcp/mcp"

	"github.com/mark3labs/mcp-go/server"
)

// ServerConfig содержит конфигурацию сервера
type ServerConfig struct {
	Transport     string
	TestMode      bool
	ClickhouseURL string
	Username      string
	Password      string
	Database      string
	Secure        bool
	Port          int
}

// Server инкапсулирует логику запуска и настройки MCP сервера
type Server struct {
	config        ServerConfig
	mcpServer     *server.MCPServer
	tools         mcp.ToolHandler
	chClient      clickhouse.Client
	clickhouseDSN string
}

// ParseClickhouseURL разбирает URL подключения к ClickHouse
func ParseClickhouseURL(url string) (string, int, string, error) {
	// Для тестового режима вернем дефолтные значения
	if url == "" || strings.HasPrefix(url, "localhost") {
		return "localhost", 9000, "default", nil
	}

	// Удаляем протокол, если есть
	cleanURL := url
	cleanURL = strings.TrimPrefix(cleanURL, "clickhouse://")

	// Разбиваем на части: host:port/database
	parts := strings.Split(cleanURL, "/")

	var host string
	var port int = 9000             // По умолчанию
	var database string = "default" // По умолчанию

	if len(parts) > 0 {
		hostPort := parts[0]
		hostPortParts := strings.Split(hostPort, ":")

		host = hostPortParts[0]
		if len(hostPortParts) > 1 {
			_, err := fmt.Sscanf(hostPortParts[1], "%d", &port)
			if err != nil {
				port = 9000 // По умолчанию, если не удалось распарсить
			}
		}

		if len(parts) > 1 {
			database = parts[1]
		}
	}

	if host == "" {
		return "", 0, "", fmt.Errorf("неверный формат URL ClickHouse: %s", url)
	}

	return host, port, database, nil
}

// NewServer создает новый экземпляр сервера
func NewServer(config ServerConfig) (*Server, error) {
	// Настраиваем логгер
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	// Создаем сервер
	server := &Server{
		config:        config,
		clickhouseDSN: config.ClickhouseURL,
	}

	// В тестовом режиме не нужно подключаться к ClickHouse
	if config.TestMode {
		// Создаем MCP сервер
		server.mcpServer = server.createMCPServer()
		return server, nil
	}

	// Подключаемся к ClickHouse
	if err := server.connectToClickhouse(); err != nil {
		return nil, err
	}

	// Создаем обработчик инструментов
	server.tools = mcp.NewToolHandler(server.chClient)

	// Создаем MCP сервер
	server.mcpServer = server.createMCPServer()

	// Регистрируем инструменты
	mcp.RegisterTools(server.mcpServer, server.tools)

	return server, nil
}

// connectToClickhouse устанавливает соединение с ClickHouse
func (s *Server) connectToClickhouse() error {
	host, port, database, err := ParseClickhouseURL(s.config.ClickhouseURL)
	if err != nil {
		return err
	}

	slog.Info("Подключение к ClickHouse",
		"host", host,
		"port", port,
		"database", database,
	)

	// Если база данных указана в конфигурации, используем ее
	if s.config.Database != "" {
		database = s.config.Database
	}

	// Создаем клиента для подключения к ClickHouse
	client, err := clickhouse.NewClient(clickhouse.Config{
		Host:     host,
		Port:     port,
		Database: database,
		Username: s.config.Username,
		Password: s.config.Password,
		Secure:   s.config.Secure,
	})
	if err != nil {
		return fmt.Errorf("ошибка подключения к ClickHouse: %w", err)
	}

	s.chClient = client
	return nil
}

// createMCPServer создает и настраивает MCP сервер
func (s *Server) createMCPServer() *server.MCPServer {
	return server.NewMCPServer(
		"clickhouse-client",  // имя сервера
		"1.0.0",              // версия
		server.WithLogging(), // включаем логирование
	)
}

// RunTests запускает тестовые примеры
func (s *Server) RunTests() {
	slog.Info("Запуск тестовых примеров")

	fmt.Println("=== Пример запроса для получения списка баз данных ===")
	fmt.Println(`{"jsonrpc":"2.0","id":"test","method":"mcp.call","params":{"tool":"get_databases","arguments":{}}}`)

	fmt.Println("\n=== Пример запроса для получения списка таблиц ===")
	fmt.Println(`{"jsonrpc":"2.0","id":"test","method":"mcp.call","params":{"tool":"get_tables","arguments":{"database":"default"}}}`)

	fmt.Println("\n=== Пример запроса для получения схемы таблицы ===")
	fmt.Println(`{"jsonrpc":"2.0","id":"test","method":"mcp.call","params":{"tool":"get_schema","arguments":{"database":"default","table":"some_table"}}}`)

	fmt.Println("\n=== Пример запроса для выполнения SQL запроса ===")
	fmt.Println(`{"jsonrpc":"2.0","id":"test","method":"mcp.call","params":{"tool":"query","arguments":{"query":"SELECT 1 as test","limit":10}}}`)

	fmt.Println("\nЗапустите сервер без флага -test и отправьте запросы через клиент MCP")
}

// Start запускает сервер
func (s *Server) Start() error {
	if s.config.TestMode {
		s.RunTests()
		return nil
	}

	if s.config.Transport == "sse" {
		addr := fmt.Sprintf(":%d", s.config.Port)
		sseServer := server.NewSSEServer(s.mcpServer, server.WithBaseURL(fmt.Sprintf("http://localhost%s", addr)))
		slog.Info("SSE server запущен", "address", addr)
		if err := sseServer.Start(addr); err != nil {
			return fmt.Errorf("ошибка запуска SSE сервера: %w", err)
		}
	} else {
		slog.Info("Запуск ClickHouse MCP сервера через stdio")
		if err := server.ServeStdio(s.mcpServer); err != nil {
			return fmt.Errorf("ошибка запуска stdio сервера: %w", err)
		}
	}

	return nil
}

// Close закрывает соединения
func (s *Server) Close() error {
	if s.chClient != nil {
		return s.chClient.Close()
	}
	return nil
}
