# syntax=docker/dockerfile:1

FROM golang:1.24-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/aevon-server ./cmd/aevon

FROM alpine:3.19

WORKDIR /app

RUN apk add --no-cache ca-certificates && \
    adduser -D -H -u 10001 aevon && \
    mkdir -p /app/config /app/config/aggregations /app/schemas && \
    chown -R aevon:aevon /app

COPY --from=builder /out/aevon-server /app/aevon-server

USER aevon

EXPOSE 8080

# Runtime mounts:
# - /app/config/aevon.yaml (server config file)
# - /app/config/aggregations (aggregation rules directory)
# - /app/schemas (schema files directory)
ENTRYPOINT ["/app/aevon-server"]
CMD ["-config", "/app/config/aevon.yaml"]
