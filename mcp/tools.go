package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"clickhouse-mcp/clickhouse"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

// ToolHandler 定义MCP工具处理器接口
type ToolHandler interface {
	// HandleGetDatabasesTool 处理获取数据库列表请求
	HandleGetDatabasesTool(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error)

	// HandleGetTablesTool 处理获取表列表请求
	HandleGetTablesTool(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error)

	// HandleGetTableSchemaTool 处理获取表结构请求
	HandleGetTableSchemaTool(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error)

	// HandleQueryTool 处理执行SQL查询请求
	HandleQueryTool(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error)
}

// DefaultToolHandler 默认工具处理器实现
type DefaultToolHandler struct {
	client clickhouse.Client
}

// NewToolHandler 创建新的工具处理器实例
func NewToolHandler(client clickhouse.Client) ToolHandler {
	return &DefaultToolHandler{
		client: client,
	}
}

// HandleGetDatabasesTool обрабатывает запрос на получение списка баз данных
func (h *DefaultToolHandler) HandleGetDatabasesTool(
	ctx context.Context,
	request mcp.CallToolRequest,
) (*mcp.CallToolResult, error) {
	databases, err := h.client.GetDatabases(ctx)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("获取数据库错误: %s", err)), nil
	}

	// Форматируем результат в текстовый вид
	result := "ClickHouse中的数据库:\n\n"
	for i, db := range databases {
		result += fmt.Sprintf("%d. %s\n", i+1, db)
	}

	// Возвращаем результат
	return mcp.NewToolResultText(result), nil
}

// HandleGetTablesTool обрабатывает запрос на получение списка таблиц
func (h *DefaultToolHandler) HandleGetTablesTool(
	ctx context.Context,
	request mcp.CallToolRequest,
) (*mcp.CallToolResult, error) {
	arguments := request.Params.Arguments
	database, ok := arguments["database"].(string)
	if !ok {
		return mcp.NewToolResultError("必须指定'database'参数"), nil
	}

	tables, err := h.client.GetTables(ctx, database)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("获取表错误: %s", err)), nil
	}

	// Форматируем результат в текстовый вид
	result := fmt.Sprintf("数据库'%s'中的表:\n\n", database)
	if len(tables) == 0 {
		result += "未找到表"
	} else {
		for i, table := range tables {
			result += fmt.Sprintf("%d. %s\n", i+1, table)
		}
	}

	// Возвращаем результат
	return mcp.NewToolResultText(result), nil
}

// HandleGetTableSchemaTool обрабатывает запрос на получение схемы таблицы
func (h *DefaultToolHandler) HandleGetTableSchemaTool(
	ctx context.Context,
	request mcp.CallToolRequest,
) (*mcp.CallToolResult, error) {
	arguments := request.Params.Arguments
	database, ok1 := arguments["database"].(string)
	table, ok2 := arguments["table"].(string)

	if !ok1 || !ok2 {
		return mcp.NewToolResultError("必须指定'database'和'table'参数"), nil
	}

	columns, err := h.client.GetTableSchema(ctx, database, table)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("获取表结构错误: %s", err)), nil
	}

	// Форматируем результат в текстовый вид
	result := fmt.Sprintf("表'%s.%s'结构:\n\n", database, table)
	if len(columns) == 0 {
		result += "未找到列"
	} else {
		result += fmt.Sprintf("%-20s | %-30s | %s\n", "列名", "类型", "位置")
		result += strings.Repeat("-", 70) + "\n"
		for _, col := range columns {
			result += fmt.Sprintf("%-20s | %-30s | %d\n", col.Name, col.Type, col.Position)
		}
	}

	// Возвращаем результат
	return mcp.NewToolResultText(result), nil
}

// HandleQueryTool обрабатывает запрос на выполнение SQL запроса
func (h *DefaultToolHandler) HandleQueryTool(
	ctx context.Context,
	request mcp.CallToolRequest,
) (*mcp.CallToolResult, error) {
	arguments := request.Params.Arguments
	query, ok1 := arguments["query"].(string)
	limit := 100 // Значение по умолчанию

	if !ok1 {
		return mcp.NewToolResultError("必须指定'query'参数"), nil
	}

	// Извлекаем лимит, если он задан
	if limitVal, ok := arguments["limit"].(float64); ok {
		limit = int(limitVal)
	}

	// Выполняем запрос
	results, err := h.client.QueryData(ctx, query, limit)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("执行查询错误: %s", err)), nil
	}

	// Прекращаем дальнейшую обработку, если нет колонок
	if len(results.Columns) == 0 {
		return mcp.NewToolResultText("查询已执行，无结果"), nil
	}

	// Преобразуем результаты для JSON
	jsonBytes, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("格式化结果错误: %s", err)), nil
	}

	// Возвращаем результат в текстовом виде (поскольку mcp-go не имеет метода NewToolResultJSON)
	return mcp.NewToolResultText(string(jsonBytes)), nil
}

// RegisterTools регистрирует инструменты MCP
func RegisterTools(mcpServer *server.MCPServer, handler ToolHandler) {
	// Инструмент для получения списка баз данных
	mcpServer.AddTool(mcp.NewTool("get_databases",
		mcp.WithDescription("获取ClickHouse数据库列表"),
	), handler.HandleGetDatabasesTool)

	// Инструмент для получения списка таблиц
	mcpServer.AddTool(mcp.NewTool("get_tables",
		mcp.WithDescription("获取指定数据库表列表"),
		mcp.WithString("database",
			mcp.Description("数据库名称"),
			mcp.Required(),
		),
	), handler.HandleGetTablesTool)

	// Инструмент для получения схемы таблицы
	mcpServer.AddTool(mcp.NewTool("get_schema",
		mcp.WithDescription("获取指定表结构"),
		mcp.WithString("database",
			mcp.Description("数据库名称"),
		),
		mcp.WithString("table",
			mcp.Description("表名称"),
			mcp.Required(),
		),
	), handler.HandleGetTableSchemaTool)

	// Инструмент для выполнения SQL запроса
	mcpServer.AddTool(mcp.NewTool("query",
		mcp.WithDescription("执行ClickHouse SQL查询"),
		mcp.WithString("query",
			mcp.Description("要执行的SQL查询"),
		),
		mcp.WithNumber("limit",
			mcp.Description("最大返回行数(默认100)"),
		),
	), handler.HandleQueryTool)
}
