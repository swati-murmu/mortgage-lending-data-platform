ROWS ?= 20000000
PY ?= python

.PHONY: help setup generate clean marts dq load analytics run test lint

help:
	@echo "make setup      - install python deps"
	@echo "make run        - run the full pipeline locally (ROWS=$(ROWS))"
	@echo "make test       - run unit tests"
	@echo "targets: generate clean marts dq load analytics"

setup:
	pip install -r requirements.txt

generate:
	spark-submit src/generate/generate_data.py --rows $(ROWS) --output data/raw

clean:
	spark-submit src/spark/clean_transform.py --input data/raw --output data/curated

marts:
	spark-submit src/spark/build_marts.py --input data/curated --output data/marts

dq:
	spark-submit src/spark/data_quality.py --marts data/marts

load:
	spark-submit src/warehouse/load_warehouse.py --marts data/marts --target duckdb

analytics:
	$(PY) src/warehouse/run_analytics.py

run: generate clean marts dq load analytics

test:
	pytest tests/ -v
