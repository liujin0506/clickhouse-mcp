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

// Client определяет интерфейс для работы с ClickHouse
type Client interface {
	// GetDatabases возвращает список баз данных
	GetDatabases(ctx context.Context) ([]string, error)

	// GetTables возвращает список таблиц в указанной базе данных
	GetTables(ctx context.Context, database string) ([]string, error)

	// GetTableSchema возвращает схему указанной таблицы
	GetTableSchema(ctx context.Context, database, table string) ([]ColumnInfo, error)

	// QueryData выполняет запрос и возвращает результаты
	QueryData(ctx context.Context, query string, limit int) (QueryResult, error)

	// GetConnection возвращает соединение с ClickHouse
	GetConnection() driver.Conn

	// Close закрывает соединение с ClickHouse
	Close() error
}

// ColumnInfo содержит информацию о колонке таблицы
type ColumnInfo struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Position int    `json:"position"`
	IsArray  bool   `json:"is_array,omitempty"`
	IsNested bool   `json:"is_nested,omitempty"`
}

// QueryResult содержит результат выполнения запроса
type QueryResult struct {
	Columns []ColumnInfo     `json:"columns"`
	Rows    []map[string]any `json:"rows"`
}

// DefaultClient - стандартная реализация клиента ClickHouse
type DefaultClient struct {
	conn driver.Conn
}

// Config содержит настройки подключения к ClickHouse
type Config struct {
	Host     string
	Port     int
	Database string
	Username string
	Password string
	Secure   bool
}

// NewClient создает новый экземпляр клиента ClickHouse
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
		return nil, fmt.Errorf("ошибка подключения к ClickHouse: %w", err)
	}

	// Проверяем соединение
	if err := conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("ошибка проверки соединения: %w", err)
	}

	return &DefaultClient{conn: conn}, nil
}

// GetDatabases возвращает список баз данных
func (c *DefaultClient) GetDatabases(ctx context.Context) ([]string, error) {
	rows, err := c.conn.Query(ctx, "SHOW DATABASES")
	if err != nil {
		return nil, fmt.Errorf("ошибка получения списка баз данных: %w", err)
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("ошибка сканирования базы данных: %w", err)
		}
		// Пропускаем системные базы данных
		if name != "system" && name != "information_schema" {
			databases = append(databases, name)
		}
	}

	return databases, nil
}

// GetTables возвращает список таблиц в указанной базе данных
func (c *DefaultClient) GetTables(ctx context.Context, database string) ([]string, error) {
	rows, err := c.conn.Query(ctx, fmt.Sprintf("SHOW TABLES FROM %s", database))
	if err != nil {
		return nil, fmt.Errorf("ошибка получения списка таблиц: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("ошибка сканирования таблицы: %w", err)
		}
		tables = append(tables, name)
	}

	return tables, nil
}

// GetTableSchema возвращает схему указанной таблицы
func (c *DefaultClient) GetTableSchema(ctx context.Context, database, table string) ([]ColumnInfo, error) {
	query := fmt.Sprintf("DESCRIBE TABLE %s.%s", database, table)
	rows, err := c.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("ошибка получения схемы таблицы: %w", err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	position := 0
	for rows.Next() {
		position++
		var name, typ, defaultType, defaultExpression, comment, codecExpression, ttlExpression string
		if err := rows.Scan(&name, &typ, &defaultType, &defaultExpression, &comment, &codecExpression, &ttlExpression); err != nil {
			return nil, fmt.Errorf("ошибка сканирования колонки: %w", err)
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

	// Проверяем ошибки после цикла
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ошибка при получении схемы таблицы: %w", err)
	}

	return columns, nil
}

// QueryData выполняет запрос и возвращает результаты в виде массива строк
func (c *DefaultClient) QueryData(ctx context.Context, query string, limit int) (QueryResult, error) {
	// Нормализуем запрос
	cleanQuery := normalizeQuery(query)

	limitedQuery := cleanQuery
	if limit > 0 {
		// Проверяем, содержит ли запрос уже LIMIT
		if !containsLimitClause(cleanQuery) {
			limitedQuery = fmt.Sprintf("%s LIMIT %d", cleanQuery, limit)
		}
	}

	// Проверяем соединение перед выполнением запроса
	if err := c.ensureConnection(ctx); err != nil {
		return QueryResult{}, fmt.Errorf("ошибка подключения: %w", err)
	}

	rows, err := c.conn.Query(ctx, limitedQuery)
	if err != nil {
		return QueryResult{}, fmt.Errorf("ошибка выполнения запроса: %w", err)
	}
	defer rows.Close()

	// Получаем информацию о колонках
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

	// Получаем данные
	var results []map[string]any

	// Создаем набор временных переменных для сканирования результатов
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
		// Сканируем строку в предварительно созданные переменные
		if err := rows.Scan(destPointers...); err != nil {
			return QueryResult{}, fmt.Errorf("ошибка сканирования строки: %w", err)
		}

		// Создаем map для текущей строки
		row := make(map[string]any)

		// Копируем значения из временных переменных в результирующую map
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
				// Для остальных типов копируем значение как есть
				v := anyVars[i]

				// Обработка специальных типов
				switch val := v.(type) {
				case []byte:
					// Конвертируем []byte в string
					row[col.Name] = string(val)
				case []any:
					// Обрабатываем массивы
					if col.IsArray && len(val) > 0 {
						// Проверяем первый элемент массива
						if _, ok := val[0].([]byte); ok {
							// Конвертируем массив []byte в []string
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

	// Проверяем наличие ошибок после цикла
	if err := rows.Err(); err != nil {
		return QueryResult{}, fmt.Errorf("ошибка при обработке результатов: %w", err)
	}

	return QueryResult{
		Columns: columns,
		Rows:    results,
	}, nil
}

// ensureConnection проверяет состояние соединения и пытается восстановить его при необходимости
func (c *DefaultClient) ensureConnection(ctx context.Context) error {
	if err := c.conn.Ping(ctx); err != nil {
		// Если соединение потеряно, логируем ошибку
		return fmt.Errorf("потеряно соединение с ClickHouse: %w", err)
	}
	return nil
}

// normalizeQuery выполняет нормализацию SQL запроса
func normalizeQuery(query string) string {
	// Удаляем точки с запятой в конце запроса
	query = strings.TrimSpace(query)
	if len(query) > 0 && query[len(query)-1] == ';' {
		query = query[:len(query)-1]
	}

	// Удаляем лишние пробелы
	query = strings.TrimSpace(query)

	return query
}

// containsLimitClause проверяет, содержит ли запрос уже LIMIT
func containsLimitClause(query string) bool {
	// Удаляем комментарии из запроса для проверки
	queryWithoutComments := removeComments(query)
	upperQuery := strings.ToUpper(queryWithoutComments)
	return strings.Contains(upperQuery, " LIMIT ")
}

// removeComments удаляет SQL комментарии из строки запроса
func removeComments(query string) string {
	// Удаляем многострочные комментарии /* ... */
	result := query
	for {
		startIdx := strings.Index(result, "/*")
		if startIdx == -1 {
			break
		}

		endIdx := strings.Index(result[startIdx:], "*/")
		if endIdx == -1 {
			// Если нет закрывающего комментария, обрезаем строку
			result = result[:startIdx]
			break
		}

		endIdx = startIdx + endIdx + 2 // +2 для учета "*/"
		result = result[:startIdx] + " " + result[endIdx:]
	}

	// Удаляем однострочные комментарии --
	lines := strings.Split(result, "\n")
	for i, line := range lines {
		commentIdx := strings.Index(line, "--")
		if commentIdx != -1 {
			lines[i] = line[:commentIdx]
		}
	}

	return strings.Join(lines, "\n")
}

// endsWithSemicolon проверяет, заканчивается ли запрос точкой с запятой
func endsWithSemicolon(query string) bool {
	trimmed := strings.TrimSpace(query)
	return len(trimmed) > 0 && trimmed[len(trimmed)-1] == ';'
}

// GetConnection возвращает соединение с ClickHouse
func (c *DefaultClient) GetConnection() driver.Conn {
	return c.conn
}

// Close закрывает соединение с ClickHouse
func (c *DefaultClient) Close() error {
	return c.conn.Close()
}

// IsArrayType проверяет, является ли тип массивом
func IsArrayType(typeName string) bool {
	return len(typeName) >= 6 && typeName[:5] == "Array"
}

// GetBaseType возвращает базовый тип из названия типа ClickHouse
func GetBaseType(typeName string) string {
	if IsArrayType(typeName) {
		// Удаляем Array() и получаем внутренний тип
		return typeName[6 : len(typeName)-1]
	}
	return typeName
}
