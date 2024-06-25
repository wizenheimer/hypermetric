# Variables
PYTHON = python3
POETRY = poetry
DIST_DIR = src/dist
LOCK_FILE = src/poetry.lock

# Install dependencies and build the package using Poetry
.PHONY: build
build:
	@echo "Building package..."
	@cd src/cyyrus && $(POETRY) install && $(POETRY) build

# Install the built package
.PHONY: install
install: build
	@echo "Installing package..."
	@pip install $$(ls -t $(DIST_DIR)/*.whl | head -1)

# Clean build artifacts
.PHONY: clean
clean:
	@echo "Cleaning up environment and build artifacts..."
	@rm -rf $(VENV_DIR)
	@find . -type d -name '__pycache__' -exec rm -r {} +
	@find . -type d -name '*.egg-info' -exec rm -r {} +
	@rm -rf $(DIST_DIR)
	@rm -rf $(LOCK_FILE)