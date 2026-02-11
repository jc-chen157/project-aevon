.PHONY: build test test-unit test-integration test-integration-race test-all clean run fmt vet db-up db-down db-migrate db-reset

BINARY_NAME=aevon
BINARY_DIR=bin
BINARY_PATH=$(BINARY_DIR)/$(BINARY_NAME)
MAIN_PATH=./cmd/aevon
COMPOSE_FILE=deploy/compose.yaml

# ─── Build & Lint ────────────────────────────────────────────

build:
	mkdir -p $(BINARY_DIR)
	go build -o $(BINARY_PATH) $(MAIN_PATH)

test: test-unit
	@echo "Run 'make test-all' to include integration tests"

test-unit:
	go test -v ./...

clean:
	go clean
	rm -f $(BINARY_PATH)

fmt:
	go fmt ./...

vet:
	go vet ./...

# ─── Integration Tests ───────────────────────────────────────

# Run integration tests (requires database)
test-integration: db-migrate
	@echo "Running integration tests..."
	go test -v -tags=integration ./test/integration/...

# Run integration tests with race detector
test-integration-race: db-migrate
	@echo "Running integration tests with race detector..."
	go test -v -race -tags=integration ./test/integration/...

# Run all tests (unit + integration)
test-all: test-unit test-integration

# ─── Database ────────────────────────────────────────────────

# Start PostgreSQL container and wait for it to be healthy
db-up:
	docker compose -f $(COMPOSE_FILE) up -d
	@echo "Waiting for PostgreSQL to be ready..."
	@until docker compose -f $(COMPOSE_FILE) exec postgres pg_isready -U aevon_dev -d aevon > /dev/null 2>&1; do \
		sleep 1; \
	done
	@echo "PostgreSQL is ready"

# Stop and remove PostgreSQL container
db-down:
	docker compose -f $(COMPOSE_FILE) down

# Run all .up.sql migrations in order against the running container
db-migrate: db-up
	@echo "Running migrations..."
	@for f in $(shell ls migrations/*.up.sql | sort); do \
		echo "  Applying $$f..." && \
			docker compose -f $(COMPOSE_FILE) exec -T postgres psql -v ON_ERROR_STOP=1 -U aevon_dev -d aevon < $$f || exit 1; \
	done
	@echo "Migrations complete"

# Full reset: drop and re-run all migrations (dev only!)
db-reset: db-up
	@echo "Resetting database..."
	@for f in $(shell ls migrations/*.down.sql | sort -r); do \
		echo "  Rolling back $$f..." && \
		docker compose -f $(COMPOSE_FILE) exec -T postgres psql -U aevon_dev -d aevon < $$f || true; \
	done
	@$(MAKE) db-migrate

# Clean all data: truncates tables but keeps schema (dev only!)
db-clean: db-up
	@echo "Cleaning database data..."
	docker compose -f $(COMPOSE_FILE) exec -T postgres psql -U aevon_dev -d aevon -c "TRUNCATE TABLE events;"
	@echo "Database cleaned"

# ─── Run ─────────────────────────────────────────────────────

# Full startup: ensure DB is up, migrations are applied, then run
run: db-migrate build
	./$(BINARY_PATH)
