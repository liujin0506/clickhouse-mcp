FROM golang:1.24-alpine AS builder

WORKDIR /app

# Copy go.mod and go.sum first for caching dependencies
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the source code
COPY . .

# Build the application
RUN go build -ldflags="-s -w" -o clickhouse-mcp .

FROM alpine:latest

WORKDIR /app

# Copy the built binary from the builder stage
COPY --from=builder /app/clickhouse-mcp ./

# Создаем директорию для логов
RUN mkdir -p /app/logs

# Установка переменной окружения для порта (по умолчанию 8080)
ENV PORT=8080

# Запуск сервера через SSE с заданным портом
ENTRYPOINT ["sh", "-c", "./clickhouse-mcp -t sse -url ${CLICKHOUSE_URL:-localhost:9000/default} -user ${CLICKHOUSE_USER:-default} -password ${CLICKHOUSE_PASSWORD:-} -db ${CLICKHOUSE_DB:-} -secure=${CLICKHOUSE_SECURE:-false}"] 