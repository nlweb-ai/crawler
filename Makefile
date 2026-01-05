SHELL := /bin/bash

.PHONY: help master worker bootstrap compose-up compose-down env-check test

DEFAULT_GOAL := help

help:
	@echo "Makefile commands:"
	@echo "  make master        # Run master (API + scheduler)"
	@echo "  make bootstrap     # Bootstrap local Python virtualenv and install dependencies"
	@echo "  make worker        # Run a worker process"
	@echo "  make test          # Run local test suite (unit tests)"
	@echo "  make compose-up    # docker-compose up --build"
	@echo "  make compose-down  # docker-compose down -v"
	@echo "  make env-check     # Ensure .env exists"

master: env-check
	@echo "Starting master..."
	@$(PYTHON) testing/start_api_server.py

worker: env-check
	@echo "Starting worker..."
	@$(PYTHON) testing/start_worker.py

.PHONY: bootstrap

# Bootstrap local Python virtualenv and install dependencies
VENV := .venv
PIP := $(VENV)/bin/pip
PYTHON := $(VENV)/bin/python3

bootstrap:
	@echo "Bootstrapping local Python virtualenv at $(VENV)"
	@echo "Checking Python version (>= 3.12 recommended)..."
	@python3.12 -c "import sys; v=sys.version_info; print('Python version OK' if (v.major>3 or (v.major==3 and v.minor>=12)) else 'WARNING: Python 3.12+ recommended; proceeding, some features may not work as expected')"
	@if [ ! -d "$(VENV)" ]; then python3.12 -m venv "$(VENV)"; fi
	@echo "Upgrading pip and wheel"
	@$(PIP) install --upgrade pip setuptools wheel
	@echo "Installing Cython (compatible version) to avoid build issues for C extensions"
	@$(PIP) install "Cython<3"
	@echo "Installing project dependencies"
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

test:
	@echo "Running local test suite..."
	@echo ""
	@echo "Running unit tests..."
	@if [ -d "$(VENV)" ]; then \
		PYTHON_CMD="$(VENV)/bin/python3"; \
	else \
		PYTHON_CMD="python3"; \
	fi; \
	$$PYTHON_CMD -m pytest code/tests/ -v || \
	{ echo ""; echo "Note: pytest not installed, falling back to direct test execution..."; \
	  $$PYTHON_CMD code/tests/test_master.py; }
