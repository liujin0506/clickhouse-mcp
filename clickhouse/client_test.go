package clickhouse

import (
	"testing"
)

func TestIsArrayType(t *testing.T) {
	tests := []struct {
		name     string
		typeName string
		want     bool
	}{
		{
			name:     "Простой тип",
			typeName: "String",
			want:     false,
		},
		{
			name:     "Массив строк",
			typeName: "Array(String)",
			want:     true,
		},
		{
			name:     "Массив целых чисел",
			typeName: "Array(UInt64)",
			want:     true,
		},
		{
			name:     "Вложенный массив",
			typeName: "Array(Array(Int32))",
			want:     true,
		},
		{
			name:     "Пустая строка",
			typeName: "",
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsArrayType(tt.typeName)
			if got != tt.want {
				t.Errorf("IsArrayType() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetBaseType(t *testing.T) {
	tests := []struct {
		name     string
		typeName string
		want     string
	}{
		{
			name:     "Простой тип",
			typeName: "String",
			want:     "String",
		},
		{
			name:     "Массив строк",
			typeName: "Array(String)",
			want:     "String",
		},
		{
			name:     "Массив целых чисел",
			typeName: "Array(UInt64)",
			want:     "UInt64",
		},
		{
			name:     "Вложенный массив",
			typeName: "Array(Array(Int32))",
			want:     "Array(Int32)",
		},
		{
			name:     "Пустая строка",
			typeName: "",
			want:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetBaseType(tt.typeName)
			if got != tt.want {
				t.Errorf("GetBaseType() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNormalizeQuery(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  string
	}{
		{
			name:  "Запрос без точки с запятой",
			query: "SELECT 1",
			want:  "SELECT 1",
		},
		{
			name:  "Запрос с точкой с запятой",
			query: "SELECT 1;",
			want:  "SELECT 1",
		},
		{
			name:  "Запрос с лишними пробелами",
			query: "  SELECT   1  ; ",
			want:  "SELECT   1",
		},
		{
			name:  "Пустой запрос",
			query: "",
			want:  "",
		},
		{
			name:  "Только точка с запятой",
			query: ";",
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeQuery(tt.query)
			if got != tt.want {
				t.Errorf("normalizeQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestContainsLimitClause(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  bool
	}{
		{
			name:  "Запрос без LIMIT",
			query: "SELECT * FROM table",
			want:  false,
		},
		{
			name:  "Запрос с LIMIT",
			query: "SELECT * FROM table LIMIT 10",
			want:  true,
		},
		{
			name:  "Запрос с limit в нижнем регистре",
			query: "select * from table limit 100",
			want:  true,
		},
		{
			name:  "LIMIT в комментарии",
			query: "SELECT * FROM table /* LIMIT 10 */",
			want:  false,
		},
		{
			name:  "LIMIT в строке",
			query: "SELECT 'LIMIT 10' FROM table",
			want:  false,
		},
		{
			name:  "Пустой запрос",
			query: "",
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := containsLimitClause(tt.query)
			if got != tt.want {
				t.Errorf("containsLimitClause() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEndsWithSemicolon(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  bool
	}{
		{
			name:  "Запрос без точки с запятой",
			query: "SELECT 1",
			want:  false,
		},
		{
			name:  "Запрос с точкой с запятой",
			query: "SELECT 1;",
			want:  true,
		},
		{
			name:  "Запрос с точкой с запятой и пробелами",
			query: "SELECT 1  ;  ",
			want:  true,
		},
		{
			name:  "Пустой запрос",
			query: "",
			want:  false,
		},
		{
			name:  "Только точка с запятой",
			query: ";",
			want:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := endsWithSemicolon(tt.query)
			if got != tt.want {
				t.Errorf("endsWithSemicolon() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRemoveComments(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  string
	}{
		{
			name:  "Запрос без комментариев",
			query: "SELECT * FROM table",
			want:  "SELECT * FROM table",
		},
		{
			name:  "Запрос с многострочным комментарием",
			query: "SELECT * FROM table /* это комментарий */ WHERE id = 1",
			want:  "SELECT * FROM table   WHERE id = 1",
		},
		{
			name:  "Запрос с многострочным комментарием в конце",
			query: "SELECT * FROM table /* это комментарий */",
			want:  "SELECT * FROM table  ",
		},
		{
			name:  "Запрос с незакрытым многострочным комментарием",
			query: "SELECT * FROM table /* незакрытый комментарий",
			want:  "SELECT * FROM table ",
		},
		{
			name:  "Запрос с однострочным комментарием",
			query: "SELECT * FROM table -- это комментарий\nWHERE id = 1",
			want:  "SELECT * FROM table \nWHERE id = 1",
		},
		{
			name:  "Запрос с несколькими комментариями",
			query: "SELECT * /* внутри */ FROM table -- конец строки\nWHERE /* условие */ id = 1",
			want:  "SELECT *   FROM table \nWHERE   id = 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := removeComments(tt.query)
			if got != tt.want {
				t.Errorf("removeComments() = %v, want %v", got, tt.want)
			}
		})
	}
}
