SHELL := /bin/bash
ROOT := $(abspath $(CURDIR))
BIN_DIR := $(ROOT)/bin
UI_DIR := $(ROOT)/ui
DIST_DIR := $(ROOT)/internal/web/dist
BINARY := habitat

.PHONY: dev build build-ui build-backend clean tidy fmt

dev: $(DIST_DIR) ## Run backend (with auto-reload) and UI dev server via shoreman
	./scripts/shoreman.sh Procfile

$(DIST_DIR): ## Ensure dist dir exists (full build or placeholder)
	@mkdir -p $(DIST_DIR)
	@if [ ! -f $(DIST_DIR)/index.html ]; then \
		echo '<!doctype html><html><body>dev</body></html>' > $(DIST_DIR)/index.html; \
	fi

build: build-ui build-backend ## Build UI assets and Go binary

build-ui: ## Compile the Solid dashboard bundle
	cd $(UI_DIR) && npm install
	cd $(UI_DIR) && npm run build

build-backend: ## Build the Habitat dashboard server binary
	@mkdir -p $(BIN_DIR)
	cd $(ROOT) && go build -o $(BIN_DIR)/$(BINARY) ./cmd/habitat

fmt: ## Format Go sources
	cd $(ROOT) && gofmt -w $$(find . -name '*.go' -not -path './node_modules/*')

tidy: ## Clean go module dependencies
	cd $(ROOT) && go mod tidy
