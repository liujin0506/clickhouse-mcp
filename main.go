package main

import (
	"flag"
	"log/slog"
	"os"

	"clickhouse-mcp/app"
)

func main() {
	// Определение флагов командной строки
	var (
		transport     string
		testMode      bool
		clickhouseURL string
		username      string
		password      string
		database      string
		secure        bool
		port          int
	)

	// Настройки транспорта и тестового режима
	flag.StringVar(&transport, "t", "stdio", "Transport type (stdio or sse)")
	flag.StringVar(&transport, "transport", "stdio", "Transport type (stdio or sse)")
	flag.BoolVar(&testMode, "test", false, "Run in test mode")
	flag.IntVar(&port, "port", 8082, "Port for SSE server")

	// Настройки подключения к ClickHouse
	flag.StringVar(&clickhouseURL, "url", "localhost:9000/default", "ClickHouse URL (format: host:port/database)")
	flag.StringVar(&username, "user", "default", "ClickHouse username")
	flag.StringVar(&password, "password", "", "ClickHouse password")
	flag.StringVar(&database, "db", "", "ClickHouse database (overrides database in URL)")
	flag.BoolVar(&secure, "secure", false, "Use TLS connection")

	flag.Parse()

	// Настраиваем текстовый логгер
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	// Проверка обязательных параметров
	if clickhouseURL == "" {
		slog.Error("URL ClickHouse не указан. Используйте флаг -url")
		os.Exit(1)
	}

	// Конфигурация сервера
	config := app.ServerConfig{
		Transport:     transport,
		TestMode:      testMode,
		ClickhouseURL: clickhouseURL,
		Username:      username,
		Password:      password,
		Database:      database,
		Secure:        secure,
		Port:          port,
	}

	// Создаем и запускаем сервер
	server, err := app.NewServer(config)
	if err != nil {
		slog.Error("Ошибка создания сервера", "err", err)
		os.Exit(1)
	}
	defer server.Close()

	if err := server.Start(); err != nil {
		slog.Error("Ошибка сервера", "err", err)
		os.Exit(1)
	}
}
