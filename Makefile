.PHONY: install lint test format run
install:
	pip install -e ".[dev]"
format:
	ruff format kairos_macro tests
lint:
	ruff check kairos_macro tests
test:
	pytest -q
run:
	python -m kairos_macro
