###############################################################################
# People Analytics Lakehouse Platform
#
# Development Task Runner
###############################################################################

.PHONY: help \
	setup install clean format lint test \
	db-create db-seed \
	simulate simulate-full validate \
	extract validate-raw upload-s3 raw-pipeline full-refresh \
	bronze-test bronze-all bronze-verify emr-package emr-compat-test \
	docker-up docker-down docker-logs


###############################################################################
# Help
###############################################################################

help:
	@echo ""
	@echo "People Analytics Lakehouse Platform"
	@echo ""
	@echo "Available Commands"
	@echo ""
	@echo "Development"
	@echo "  make setup"
	@echo "  make install"
	@echo "  make clean"
	@echo "  make format"
	@echo "  make lint"
	@echo "  make test"
	@echo ""
	@echo "Database"
	@echo "  make db-create"
	@echo "  make db-seed"
	@echo ""
	@echo "Simulation"
	@echo "  make simulate"
	@echo "  make simulate-full"
	@echo "  make validate"
	@echo ""
	@echo "Raw ETL"
	@echo "  make extract"
	@echo "  make validate-raw"
	@echo "  make upload-s3"
	@echo "  make raw-pipeline"
	@echo "  make bronze-test"
	@echo "  make bronze-all BATCH_ID=<validated-batch-id>"
	@echo "  make bronze-verify BATCH_ID=<validated-batch-id>"
	@echo "  make emr-package"
	@echo "  make emr-compat-test"
	@echo ""
	@echo "Platform"
	@echo "  make full-refresh"
	@echo ""
	@echo "Docker"
	@echo "  make docker-up"
	@echo "  make docker-down"
	@echo "  make docker-logs"
	@echo ""


###############################################################################
# Development
###############################################################################

setup:
	pip install -r requirements.txt

install:
	pip install -r requirements.txt


###############################################################################
# Cleanup
#
# Uses Python rather than Unix 'find' so it also works from Windows
# environments where GNU find may not be available.
###############################################################################

clean:
	python -c "import shutil; from pathlib import Path; [shutil.rmtree(p, ignore_errors=True) for p in Path('.').rglob('__pycache__')]; print('Python cache directories removed.')"


###############################################################################
# Formatting / Linting
###############################################################################

format:
	black .

lint:
	ruff check .


###############################################################################
# Testing
###############################################################################

test:
	pytest


###############################################################################
# Database
###############################################################################

db-create:
	python -m database.create_schema

db-seed:
	python -m database.seed


###############################################################################
# Simulation
###############################################################################

simulate:
	python main.py simulate

simulate-full:
	python main.py simulate --full-refresh

validate:
	python main.py validate


###############################################################################
# Raw ETL
###############################################################################

extract:
	python main.py extract

validate-raw:
	python main.py validate-raw

upload-s3:
	python main.py upload-s3


###############################################################################
# Raw Pipeline
#
# PostgreSQL
#     ↓
# Raw Parquet
#     ↓
# Raw extraction validation
#     ↓
# Amazon S3 raw zone
###############################################################################

raw-pipeline:
	python main.py raw-pipeline


###############################################################################
# Bronze
###############################################################################

bronze-test:
	docker compose run --rm --build spark-tests

bronze-all:
	docker compose run --rm spark-bronze --all-tables --batch-id $(BATCH_ID)

bronze-verify:
	docker compose run --rm spark-bronze --all-tables --batch-id $(BATCH_ID) --verify-existing


###############################################################################
# Amazon EMR preparation - local build/test only; no AWS calls
###############################################################################

emr-package:
	docker compose run --rm emr-compat-tests python scripts/build_emr_bundle.py

emr-compat-test:
	docker compose run --rm --build emr-compat-tests


###############################################################################
# Full Platform Refresh - Current Implemented Scope
#
# Simulation full refresh
#     ↓
# Operational quality validation
#     ↓
# PostgreSQL extraction
#     ↓
# Raw extraction validation
#     ↓
# Amazon S3 raw upload
#
# Bronze / Silver / Gold are intentionally excluded until those layers
# are implemented.
###############################################################################

full-refresh:
	python main.py full-refresh


###############################################################################
# Docker
###############################################################################

docker-up:
	docker compose up -d

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f
