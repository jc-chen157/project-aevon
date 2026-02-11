# Build Stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install build dependencies for CGO (sqlite3)
RUN apk add --no-cache build-base

# Copy go mod and sum files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary
RUN CGO_ENABLED=1 GOOS=linux go build -o aevon-server cmd/aevon/main.go

# Final Stage
FROM alpine:3.19

WORKDIR /app

# Install dependencies needing for CGO (sqlite) if utilizing it, 
# though git-sync scenario might lean towards postgres in prod.
# For consistency with current dev setup, we'll ensure sqlite libs are present if needed,
# or just rely on the static build.
RUN apk add --no-cache ca-certificates

# Copy binary from builder
COPY --from=builder /app/aevon-server .

# Create directory for schemas (will be mounted)
RUN mkdir -p /app/schemas

# Expose port
EXPOSE 8080

# Run
CMD ["./aevon-server"]
