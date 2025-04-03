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

// ServerConfig 包含服务器配置
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

// Server 封装了MCP服务器的启动和配置逻辑
type Server struct {
	config        ServerConfig
	mcpServer     *server.MCPServer
	tools         mcp.ToolHandler
	chClient      clickhouse.Client
	clickhouseDSN string
}

// ParseClickhouseURL 解析ClickHouse连接URL
func ParseClickhouseURL(url string) (string, int, string, error) {
	// 测试模式返回默认值
	if url == "" || strings.HasPrefix(url, "localhost") {
		return "localhost", 9000, "default", nil
	}

	// 移除协议前缀
	cleanURL := url
	cleanURL = strings.TrimPrefix(cleanURL, "clickhouse://")

	// 分割为host:port/database格式
	parts := strings.Split(cleanURL, "/")

	var host string
	var port int = 9000             // 默认端口
	var database string = "default" // 默认数据库

	if len(parts) > 0 {
		hostPort := parts[0]
		hostPortParts := strings.Split(hostPort, ":")

		host = hostPortParts[0]
		if len(hostPortParts) > 1 {
			_, err := fmt.Sscanf(hostPortParts[1], "%d", &port)
			if err != nil {
				port = 9000 // 解析失败使用默认值
			}
		}

		if len(parts) > 1 {
			database = parts[1]
		}
	}

	if host == "" {
		return "", 0, "", fmt.Errorf("无效的ClickHouse URL格式: %s", url)
	}

	return host, port, database, nil
}

// NewServer 创建新的服务器实例
func NewServer(config ServerConfig) (*Server, error) {
	// 配置日志
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	// 创建服务器
	server := &Server{
		config:        config,
		clickhouseDSN: config.ClickhouseURL,
	}

	// 测试模式不需要连接ClickHouse
	if config.TestMode {
		// 创建MCP服务器
		server.mcpServer = server.createMCPServer()
		return server, nil
	}

	// 连接ClickHouse
	if err := server.connectToClickhouse(); err != nil {
		return nil, err
	}

	// 创建工具处理器
	server.tools = mcp.NewToolHandler(server.chClient)

	// 创建MCP服务器
	server.mcpServer = server.createMCPServer()

	// 注册工具
	mcp.RegisterTools(server.mcpServer, server.tools)

	return server, nil
}

// connectToClickhouse 建立与ClickHouse的连接
func (s *Server) connectToClickhouse() error {
	host, port, database, err := ParseClickhouseURL(s.config.ClickhouseURL)
	if err != nil {
		return err
	}

	slog.Info("连接ClickHouse",
		"host", host,
		"port", port,
		"database", database,
	)

	// 如果配置中指定了数据库则使用配置值
	if s.config.Database != "" {
		database = s.config.Database
	}

	// 创建ClickHouse客户端
	client, err := clickhouse.NewClient(clickhouse.Config{
		Host:     host,
		Port:     port,
		Database: database,
		Username: s.config.Username,
		Password: s.config.Password,
		Secure:   s.config.Secure,
	})
	if err != nil {
		return fmt.Errorf("连接ClickHouse失败: %w", err)
	}

	s.chClient = client
	return nil
}

// createMCPServer 创建并配置MCP服务器
func (s *Server) createMCPServer() *server.MCPServer {
	return server.NewMCPServer(
		"clickhouse-client",  // 服务器名称
		"1.0.0",              // 版本号
		server.WithLogging(), // 启用日志
	)
}

// RunTests 运行测试示例
func (s *Server) RunTests() {
	slog.Info("运行测试示例")

	fmt.Println("=== 获取数据库列表请求示例 ===")
	fmt.Println(`{"jsonrpc":"2.0","id":"test","method":"mcp.call","params":{"tool":"get_databases","arguments":{}}}`)

	fmt.Println("\n=== 获取表列表请求示例 ===")
	fmt.Println(`{"jsonrpc":"2.0","id":"test","method":"mcp.call","params":{"tool":"get_tables","arguments":{"database":"default"}}}`)

	fmt.Println("\n=== 获取表结构请求示例 ===")
	fmt.Println(`{"jsonrpc":"2.0","id":"test","method":"mcp.call","params":{"tool":"get_schema","arguments":{"database":"default","table":"some_table"}}}`)

	fmt.Println("\n=== 执行SQL查询请求示例 ===")
	fmt.Println(`{"jsonrpc":"2.0","id":"test","method":"mcp.call","params":{"tool":"query","arguments":{"query":"SELECT 1 as test","limit":10}}}`)

	fmt.Println("\n请在不使用-test标志的情况下启动服务器并通过MCP客户端发送请求")
}

// Start 启动服务器
func (s *Server) Start() error {
	if s.config.TestMode {
		s.RunTests()
		return nil
	}

	if s.config.Transport == "sse" {
		addr := fmt.Sprintf(":%d", s.config.Port)
		sseServer := server.NewSSEServer(s.mcpServer)
		slog.Info("SSE服务器已启动", "address", addr)
		if err := sseServer.Start(addr); err != nil {
			return fmt.Errorf("启动SSE服务器失败: %w", err)
		}
	} else {
		slog.Info("通过stdio启动ClickHouse MCP服务器")
		if err := server.ServeStdio(s.mcpServer); err != nil {
			return fmt.Errorf("启动stdio服务器失败: %w", err)
		}
	}

	return nil
}

// Close 关闭连接
func (s *Server) Close() error {
	if s.chClient != nil {
		return s.chClient.Close()
	}
	return nil
}
