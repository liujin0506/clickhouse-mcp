# ClickHouse MCP сервер

[![Go Version](https://img.shields.io/github/go-mod/go-version/Headcrab/letter_mcp)](https://go.dev)
[![License](https://img.shields.io/github/license/Headcrab/letter_mcp)](LICENSE)
[![Coverage](https://codecov.io/gh/Headcrab/letter_mcp/graph/badge.svg?token=WSRWMHXMTA)](https://codecov.io/gh/Headcrab/letter_mcp)

MCP-совместимый сервер для взаимодействия с ClickHouse базами данных.

## Возможности

- Получение списка баз данных
- Получение списка таблиц в выбранной базе данных
- Получение схемы выбранной таблицы
- Выполнение SQL запросов и получение результатов
- Поддержка разных транспортов (stdio и SSE)

## Структура проекта

```
clickhouse-mcp/
├── app/            # Основная логика приложения
│   └── server.go   # Настройка и запуск сервера
├── clickhouse/     # Пакет для работы с ClickHouse
│   └── client.go   # Клиент ClickHouse
├── mcp/            # Работа с протоколом MCP
│   └── tools.go    # Инструменты MCP
└── main.go         # Точка входа
```

## Использование

### Сборка

```bash
go build -o clickhouse-mcp
```

### Запуск

Запуск через stdio (по умолчанию):

```bash
./clickhouse-mcp -url localhost:9000/default -user default -password yourpassword
```

Запуск через SSE:

```bash
./clickhouse-mcp -t sse -url localhost:9000/default -user default -password yourpassword
```

Запуск в тестовом режиме:

```bash
./clickhouse-mcp -test
```

### Запуск в Docker

```bash
docker build -t clickhouse-mcp .
docker run -d -p 8080:8080 --name clickhouse-mcp \
  -e CLICKHOUSE_URL=host.docker.internal:9000/default \
  -e CLICKHOUSE_USER=default \
  -e CLICKHOUSE_PASSWORD=yourpassword \
  clickhouse-mcp:latest
```

```powershell
$env:PORT=8082; $env:CLICKHOUSE_URL="host.docker.internal:9000"; $env:CLICKHOUSE_USER="default" ; $env:CLICKHOUSE_PASSWORD="yourpassword"; $env:CLICKHOUSE_DB="default"; $env:CLICKHOUSE_SECURE=false; docker-compose up -d
```

### Запуск с Docker Compose

```bash
# Запуск со стандартным портом 8080
docker-compose up -d

# Запуск с пользовательским портом и другими параметрами
PORT=9090 CLICKHOUSE_URL=host.docker.internal:9000/mydatabase docker-compose up -d
```

## Параметры командной строки

- `-t, -transport`: Тип транспорта (stdio или sse), по умолчанию stdio
- `-test`: Запуск в тестовом режиме (показывает примеры запросов)
- `-url`: URL ClickHouse в формате хост:порт/база_данных
- `-user`: Имя пользователя ClickHouse, по умолчанию "default"
- `-password`: Пароль пользователя ClickHouse
- `-db`: База данных ClickHouse (переопределяет базу в URL)
- `-secure`: Использовать TLS соединение

## Формат запросов и ответов

### Запрос на получение списка баз данных

```json
{
  "jsonrpc": "2.0",
  "id": "test",
  "method": "mcp.call",
  "params": {
    "tool": "get_databases",
    "arguments": {}
  }
}
```

### Запрос на получение списка таблиц

```json
{
  "jsonrpc": "2.0",
  "id": "test",
  "method": "mcp.call",
  "params": {
    "tool": "get_tables",
    "arguments": {
      "database": "default"
    }
  }
}
```

### Запрос на получение схемы таблицы

```json
{
  "jsonrpc": "2.0",
  "id": "test",
  "method": "mcp.call",
  "params": {
    "tool": "get_schema",
    "arguments": {
      "database": "default",
      "table": "my_table"
    }
  }
}
```

### Запрос на выполнение SQL запроса

```json
{
  "jsonrpc": "2.0",
  "id": "test",
  "method": "mcp.call",
  "params": {
    "tool": "query",
    "arguments": {
      "query": "SELECT * FROM default.my_table",
      "limit": 10
    }
  }
}
```

## Настройка MCP клиента

```json
{
  "mcpServers": {
    "clickhouse": {
      "command": "/path/to/clickhouse-mcp",
      "args": ["-url", "localhost:9000/default", "-user", "default", "-password", "yourpassword"],
      "disabled": false,
      "alwaysAllow": []
    }
  }
}
```

## Настройка MCP клиента c SSE

```json
{
  "mcpServers": {
    "clickhouse": {
      "url": "http://localhost:8080/sse",
      "env": {
        "API_KEY": ""
      }
    }
  }
}
```

## Лицензия

MIT License. См. файл [LICENSE](LICENSE) для подробностей.

## Вклад в проект

1. Форкните репозиторий
2. Создайте ветку для ваших изменений
3. Внесите изменения и создайте pull request

## Контакты

Создайте issue в репозитории для сообщения о проблемах или предложений по улучшению.

## Спасибо

- [@Headcrab](https://github.com/Headcrab)
