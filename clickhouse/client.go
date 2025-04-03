package clickhouse

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Client 定义ClickHouse客户端接口
type Client interface {
	// GetDatabases 获取数据库列表
	GetDatabases(ctx context.Context) ([]string, error)

	// GetTables 获取指定数据库的表列表
	GetTables(ctx context.Context, database string) ([]string, error)

	// GetTableSchema 获取指定表结构
	GetTableSchema(ctx context.Context, database, table string) ([]ColumnInfo, error)

	// QueryData 执行查询并返回结果
	QueryData(ctx context.Context, query string, limit int) (QueryResult, error)

	// GetConnection 获取ClickHouse连接
	GetConnection() driver.Conn

	// Close 关闭连接
	Close() error
}

// ColumnInfo 包含表列信息
type ColumnInfo struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Position int    `json:"position"`
	IsArray  bool   `json:"is_array,omitempty"`
	IsNested bool   `json:"is_nested,omitempty"`
}

// QueryResult 包含查询执行结果
type QueryResult struct {
	Columns []ColumnInfo     `json:"columns"`
	Rows    []map[string]any `json:"rows"`
}

// DefaultClient ClickHouse客户端默认实现
type DefaultClient struct {
	conn driver.Conn
}

// Config 包含ClickHouse连接配置
type Config struct {
	Host     string
	Port     int
	Database string
	Username string
	Password string
	Secure   bool
}

// NewClient 创建ClickHouse客户端实例
func NewClient(cfg Config) (Client, error) {
	opts := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		DialTimeout:     5 * time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 10 * time.Minute,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Settings: clickhouse.Settings{
			"allow_experimental_object_type":             1,
			"output_format_json_named_tuples_as_objects": 1,
			"allow_suspicious_low_cardinality_types":     1,
			"format_csv_delimiter":                       ",",
		},
		Debug: false,
	}

	if cfg.Secure {
		opts.TLS = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("连接ClickHouse失败: %w", err)
	}

	// 检查连接
	if err := conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("连接检查失败: %w", err)
	}

	return &DefaultClient{conn: conn}, nil
}

// GetDatabases 获取数据库列表
func (c *DefaultClient) GetDatabases(ctx context.Context) ([]string, error) {
	rows, err := c.conn.Query(ctx, "SHOW DATABASES")
	if err != nil {
		return nil, fmt.Errorf("获取数据库列表失败: %w", err)
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("数据库扫描失败: %w", err)
		}
		// 跳过系统数据库
		if name != "system" && name != "information_schema" {
			databases = append(databases, name)
		}
	}

	return databases, nil
}

// GetTables 获取指定数据库的表列表
func (c *DefaultClient) GetTables(ctx context.Context, database string) ([]string, error) {
	rows, err := c.conn.Query(ctx, fmt.Sprintf("SHOW TABLES FROM %s", database))
	if err != nil {
		return nil, fmt.Errorf("获取表列表失败: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("表扫描失败: %w", err)
		}
		tables = append(tables, name)
	}

	return tables, nil
}

// GetTableSchema 获取指定表结构
func (c *DefaultClient) GetTableSchema(ctx context.Context, database, table string) ([]ColumnInfo, error) {
	query := fmt.Sprintf("DESCRIBE TABLE %s.%s", database, table)
	rows, err := c.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("获取表结构失败: %w", err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	position := 0
	for rows.Next() {
		position++
		var name, typ, defaultType, defaultExpression, comment, codecExpression, ttlExpression string
		if err := rows.Scan(&name, &typ, &defaultType, &defaultExpression, &comment, &codecExpression, &ttlExpression); err != nil {
			return nil, fmt.Errorf("列扫描失败: %w", err)
		}

		isArray := IsArrayType(typ)
		isNested := len(typ) >= 7 && typ[:6] == "Nested"

		columns = append(columns, ColumnInfo{
			Name:     name,
			Type:     typ,
			Position: position,
			IsArray:  isArray,
			IsNested: isNested,
		})
	}

	// 检查循环后的错误
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("获取表结构时发生错误: %w", err)
	}

	return columns, nil
}

// QueryData 执行查询并返回结果
func (c *DefaultClient) QueryData(ctx context.Context, query string, limit int) (QueryResult, error) {
	// 规范化查询
	cleanQuery := normalizeQuery(query)

	limitedQuery := cleanQuery
	if limit > 0 {
		// 检查是否已包含LIMIT
		if !containsLimitClause(cleanQuery) {
			limitedQuery = fmt.Sprintf("%s LIMIT %d", cleanQuery, limit)
		}
	}

	// 执行前检查连接
	if err := c.ensureConnection(ctx); err != nil {
		return QueryResult{}, fmt.Errorf("连接错误: %w", err)
	}

	rows, err := c.conn.Query(ctx, limitedQuery)
	if err != nil {
		return QueryResult{}, fmt.Errorf("查询执行失败: %w", err)
	}
	defer rows.Close()

	// 获取列信息
	columnTypes := rows.ColumnTypes()
	columnNames := rows.Columns()

	var columns []ColumnInfo
	for i, ct := range columnTypes {
		dbType := ct.DatabaseTypeName()
		isArray := IsArrayType(dbType)
		isNested := len(dbType) >= 7 && dbType[:6] == "Nested"

		columns = append(columns, ColumnInfo{
			Name:     ct.Name(),
			Type:     dbType,
			Position: i + 1,
			IsArray:  isArray,
			IsNested: isNested,
		})
	}

	// 获取数据
	var results []map[string]any

	// 创建临时变量用于扫描结果
	destPointers := make([]any, len(columnNames))
	stringVars := make([]string, len(columnNames))
	intVars := make([]int64, len(columnNames))
	uintVars := make([]uint64, len(columnNames))
	floatVars := make([]float64, len(columnNames))
	boolVars := make([]bool, len(columnNames))
	timeVars := make([]time.Time, len(columnNames))
	anyVars := make([]any, len(columnNames))

	for i, col := range columns {
		switch col.Type {
		case "String":
			destPointers[i] = &stringVars[i]
		case "UInt8", "UInt16", "UInt32", "UInt64":
			destPointers[i] = &uintVars[i]
		case "Int8", "Int16", "Int32", "Int64":
			destPointers[i] = &intVars[i]
		case "Float32", "Float64":
			destPointers[i] = &floatVars[i]
		case "Bool":
			destPointers[i] = &boolVars[i]
		case "Date", "DateTime":
			destPointers[i] = &timeVars[i]
		default:
			destPointers[i] = &anyVars[i]
		}
	}

	for rows.Next() {
		// 扫描行到预定义变量
		if err := rows.Scan(destPointers...); err != nil {
			return QueryResult{}, fmt.Errorf("行扫描失败: %w", err)
		}

		// 创建当前行的map
		row := make(map[string]any)

		// 复制值到结果map
		for i, col := range columns {
			switch col.Type {
			case "String":
				row[col.Name] = stringVars[i]
			case "UInt8", "UInt16", "UInt32", "UInt64":
				row[col.Name] = uintVars[i]
			case "Int8", "Int16", "Int32", "Int64":
				row[col.Name] = intVars[i]
			case "Float32", "Float64":
				row[col.Name] = floatVars[i]
			case "Bool":
				row[col.Name] = boolVars[i]
			case "Date", "DateTime":
				row[col.Name] = timeVars[i].Format(time.RFC3339)
			default:
				// 处理其他类型
				v := anyVars[i]

				switch val := v.(type) {
				case []byte:
					row[col.Name] = string(val)
				case []any:
					if col.IsArray && len(val) > 0 {
						if _, ok := val[0].([]byte); ok {
							strArray := make([]string, len(val))
							for j, item := range val {
								if byteItem, ok := item.([]byte); ok {
									strArray[j] = string(byteItem)
								} else {
									strArray[j] = fmt.Sprint(item)
								}
							}
							row[col.Name] = strArray
						} else {
							row[col.Name] = val
						}
					} else {
						row[col.Name] = val
					}
				default:
					row[col.Name] = v
				}
			}
		}

		results = append(results, row)
	}

	// 检查结果处理错误
	if err := rows.Err(); err != nil {
		return QueryResult{}, fmt.Errorf("结果处理错误: %w", err)
	}

	return QueryResult{
		Columns: columns,
		Rows:    results,
	}, nil
}

// ensureConnection 检查并维持连接
func (c *DefaultClient) ensureConnection(ctx context.Context) error {
	if err := c.conn.Ping(ctx); err != nil {
		return fmt.Errorf("ClickHouse连接丢失: %w", err)
	}
	return nil
}

// normalizeQuery 规范化SQL查询
func normalizeQuery(query string) string {
	// 去除末尾分号
	query = strings.TrimSpace(query)
	if len(query) > 0 && query[len(query)-1] == ';' {
		query = query[:len(query)-1]
	}

	// 去除多余空格
	query = strings.TrimSpace(query)

	return query
}

// containsLimitClause 检查是否包含LIMIT子句
func containsLimitClause(query string) bool {
	queryWithoutComments := removeComments(query)
	upperQuery := strings.ToUpper(queryWithoutComments)
	return strings.Contains(upperQuery, " LIMIT ")
}

// removeComments 移除SQL注释
func removeComments(query string) string {
	result := query
	for {
		startIdx := strings.Index(result, "/*")
		if startIdx == -1 {
			break
		}

		endIdx := strings.Index(result[startIdx:], "*/")
		if endIdx == -1 {
			result = result[:startIdx]
			break
		}

		endIdx = startIdx + endIdx + 2
		result = result[:startIdx] + " " + result[endIdx:]
	}

	lines := strings.Split(result, "\n")
	for i, line := range lines {
		commentIdx := strings.Index(line, "--")
		if commentIdx != -1 {
			lines[i] = line[:commentIdx]
		}
	}

	return strings.Join(lines, "\n")
}

// endsWithSemicolon 检查是否以分号结尾
func endsWithSemicolon(query string) bool {
	trimmed := strings.TrimSpace(query)
	return len(trimmed) > 0 && trimmed[len(trimmed)-1] == ';'
}

// GetConnection 获取ClickHouse连接
func (c *DefaultClient) GetConnection() driver.Conn {
	return c.conn
}

// Close 关闭连接
func (c *DefaultClient) Close() error {
	return c.conn.Close()
}

// IsArrayType 检查是否为数组类型
func IsArrayType(typeName string) bool {
	return len(typeName) >= 6 && typeName[:5] == "Array"
}

// GetBaseType 获取基础类型
func GetBaseType(typeName string) string {
	if IsArrayType(typeName) {
		return typeName[6 : len(typeName)-1]
	}
	return typeName
}
