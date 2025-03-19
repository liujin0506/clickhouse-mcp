package app

import (
	"testing"
)

func TestParseClickhouseURL(t *testing.T) {
	tests := []struct {
		name         string
		url          string
		expectedHost string
		expectedPort int
		expectedDB   string
		expectError  bool
	}{
		{
			name:         "Пустой URL",
			url:          "",
			expectedHost: "localhost",
			expectedPort: 9000,
			expectedDB:   "default",
			expectError:  false,
		},
		{
			name:         "Только localhost",
			url:          "localhost",
			expectedHost: "localhost",
			expectedPort: 9000,
			expectedDB:   "default",
			expectError:  false,
		},
		{
			name:         "Полный URL с базой данных",
			url:          "example.com:8123/test_db",
			expectedHost: "example.com",
			expectedPort: 8123,
			expectedDB:   "test_db",
			expectError:  false,
		},
		{
			name:         "URL с протоколом",
			url:          "clickhouse://myhost:9000/mydb",
			expectedHost: "myhost",
			expectedPort: 9000,
			expectedDB:   "mydb",
			expectError:  false,
		},
		{
			name:         "URL без порта",
			url:          "custom-host/custom-db",
			expectedHost: "custom-host",
			expectedPort: 9000,
			expectedDB:   "custom-db",
			expectError:  false,
		},
		{
			name:         "URL с портом, без базы данных",
			url:          "clickhouse:5555",
			expectedHost: "clickhouse",
			expectedPort: 5555,
			expectedDB:   "default",
			expectError:  false,
		},
		{
			name:        "Некорректный URL",
			url:         ":",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, port, db, err := ParseClickhouseURL(tt.url)

			// Проверяем ошибку
			if (err != nil) != tt.expectError {
				t.Errorf("ParseClickhouseURL() error = %v, expectError = %v", err, tt.expectError)
				return
			}

			// Если ожидаем ошибку, не проверяем результаты
			if tt.expectError {
				return
			}

			// Проверяем результаты
			if host != tt.expectedHost {
				t.Errorf("host = %v, expectedHost = %v", host, tt.expectedHost)
			}
			if port != tt.expectedPort {
				t.Errorf("port = %v, expectedPort = %v", port, tt.expectedPort)
			}
			if db != tt.expectedDB {
				t.Errorf("db = %v, expectedDB = %v", db, tt.expectedDB)
			}
		})
	}
}
