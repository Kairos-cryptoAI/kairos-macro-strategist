UV ?= uv

.PHONY: install lint format format-check typecheck security test build run all
install:
	$(UV) sync --locked
format:
	$(UV) run --locked ruff format kairos_macro tests
format-check:
	$(UV) run --locked ruff format --check kairos_macro tests
lint:
	$(UV) run --locked ruff check kairos_macro tests
typecheck:
	$(UV) run --locked mypy kairos_macro
security:
	$(UV) run --locked bandit -q -r kairos_macro -x tests
test:
	$(UV) run --locked pytest -q --tb=short
build:
	$(UV) build --no-sources
run:
	$(UV) run --locked python -m kairos_macro
all: lint format-check typecheck security test build
