SHELL := /bin/bash

.PHONY: help master worker bootstrap compose-up compose-down env-check

DEFAULT_GOAL := help

help:
	@echo "Makefile commands:"
	@echo "  make master        # Run master (API + scheduler) via uv"
	@echo "  make bootstrap     # Bootstrap local Python virtualenv and install dependencies"
	@echo "  make worker        # Run a worker process via uv"
	@echo "  make compose-up    # docker-compose up --build"
	@echo "  make compose-down  # docker-compose down -v"
	@echo "  make env-check     # Ensure .env exists"

master: env-check
	@echo "Starting master..."
	@$(UV) run testing/start_api_server.py

worker: env-check
	@echo "Starting worker..."
	@$(UV) run testing/start_worker.py

.PHONY: bootstrap

# Bootstrap local Python virtualenv and install dependencies
VENV := .venv
PIP := $(VENV)/bin/pip
UV := $(shell if [ -x $(VENV)/bin/uv ]; then echo $(VENV)/bin/uv; else echo uv; fi)

bootstrap:
	@echo "Bootstrapping local Python virtualenv at $(VENV)"
	@echo "Checking Python version (>= 3.12 recommended)..."
	@python3.12 -c "import sys; v=sys.version_info; print('Python version OK' if (v.major>3 or (v.major==3 and v.minor>=12)) else 'WARNING: Python 3.12+ recommended; proceeding, some features may not work as expected')"
	@if [ ! -d "$(VENV)" ]; then python3.12 -m venv "$(VENV)"; fi
	@echo "Upgrading pip and wheel"
	@$(PIP) install --upgrade pip setuptools wheel
	@echo "Installing Cython (compatible version) to avoid build issues for C extensions"
	@$(PIP) install "Cython<3"
	@echo "Installing uv manager and project dependencies"
	@$(PIP) install uv || true
	@if ! command -v uv >/dev/null 2>&1; then \
		if command -v brew >/dev/null 2>&1; then \
			echo "'uv' not found — installing via Homebrew..."; \
			brew install uv || echo "Failed to install 'uv' via Homebrew, please install manually."; \
		else \
			echo "'uv' not found and Homebrew not available. Install Homebrew (https://brew.sh) or install 'uv' manually."; \
		fi; \
	fi
	@$(PIP) install -r code/requirements.txt
	@echo "Bootstrap complete. Activate virtual environment with:\n  source $(VENV)/bin/activate"

compose-up:
	@echo "Starting services with docker-compose..."
	@docker-compose up --build

compose-down:
	@echo "Stopping docker-compose and removing volumes..."
	@docker-compose down -v

env-check:
	@if [ -f .env ]; then \
		echo ".env found"; \
	else \
		echo "WARNING: .env not found - create one from .env.example"; exit 1; \
	fi
