FROM golang:1.24.1-alpine AS builder

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
ENV PORT=8082

# Запуск сервера через SSE с заданным портом
ENTRYPOINT ["sh", "-c", "./clickhouse-mcp -t=sse -port=${PORT} -url=${CLICKHOUSE_URL:-localhost:9000} -user=${CLICKHOUSE_USER:-default} -password=${CLICKHOUSE_PASSWORD:-password} -db=${CLICKHOUSE_DB:-default} -secure=${CLICKHOUSE_SECURE:-false} -port=${PORT}"]