# Variables
VENV_DIR = .venv
PYTHON = python3
POETRY = poetry
DIST_DIR = src/dist
LOCK_FILE = src/poetry.lock

# Default target
.PHONY: all
all: venv build install

# Create virtual environment if it doesn't exist
.PHONY: venv
venv:
	@if [ ! -d "$(VENV_DIR)" ]; then \
		$(PYTHON) -m venv $(VENV_DIR); \
	fi
	@echo "Activating virtual environment and installing Poetry..."
	@. $(VENV_DIR)/bin/activate && pip install --upgrade pip && pip install $(POETRY)

# Install dependencies and build the package using Poetry
.PHONY: build
build: venv
	@echo "Building package..."
	@. $(VENV_DIR)/bin/activate && cd src/cyyrus && $(POETRY) install && $(POETRY) build

# Install the built package
.PHONY: install
install: build
	@echo "Installing package..."
	@. $(VENV_DIR)/bin/activate && pip install $$(ls -t $(DIST_DIR)/*.whl | head -1)

# Clean build artifacts
.PHONY: clean
clean:
	@echo "Cleaning up environment and build artifacts..."
	@rm -rf $(VENV_DIR)
	@find . -type d -name '__pycache__' -exec rm -r {} +
	@find . -type d -name '*.egg-info' -exec rm -r {} +
	@rm -rf $(DIST_DIR)
	@rm -rf $(LOCK_FILE)