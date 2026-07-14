###############################################################################
# People Analytics Lakehouse Platform
#
# Development Task Runner
###############################################################################

.PHONY: help setup install clean format lint test \
        db-create db-seed simulate validate \
        docker-up docker-down docker-logs \
        airflow spark api \
        terraform-init terraform-plan terraform-apply \
        docs catalogue metadata \
        etl bronze silver gold dbt \
        full-refresh

###############################################################################
# Help
###############################################################################

help:
	@echo ""
	@echo "People Analytics Lakehouse Platform"
	@echo ""
	@echo "Available Commands:"
	@echo ""
	@echo "Development"
	@echo "  make setup"
	@echo "  make install"
	@echo "  make clean"
	@echo ""
	@echo "Database"
	@echo "  make db-create"
	@echo "  make db-seed"
	@echo ""
	@echo "Simulation"
	@echo "  make simulate"
	@echo "  make validate"
	@echo ""
	@echo "ETL"
	@echo "  make etl"
	@echo "  make bronze"
	@echo "  make silver"
	@echo "  make gold"
	@echo ""
	@echo "Analytics"
	@echo "  make dbt"
	@echo ""
	@echo "Docker"
	@echo "  make docker-up"
	@echo "  make docker-down"
	@echo ""
	@echo "Terraform"
	@echo "  make terraform-init"
	@echo "  make terraform-plan"
	@echo "  make terraform-apply"
	@echo ""
	@echo "API"
	@echo "  make api"
	@echo ""
	@echo "Testing"
	@echo "  make test"
	@echo ""

###############################################################################
# Development
###############################################################################

setup:
	pip install -r requirements.txt

install:
	pip install -r requirements.txt

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +

###############################################################################
# Formatting
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
	python -m simulator.simulator

validate:
	python -m quality.validation

###############################################################################
# ETL
###############################################################################

etl:
	python -m etl.extract

bronze:
	python -m etl.bronze_loader

silver:
	python -m etl.silver_loader

gold:
	python -m etl.gold_loader

###############################################################################
# dbt
###############################################################################

dbt:
	cd dbt && dbt build

###############################################################################
# Metadata
###############################################################################

metadata:
	python -m metadata.loader

catalogue:
	python -m catalogue.report

###############################################################################
# Docker
###############################################################################

docker-up:
	docker compose up -d

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f

###############################################################################
# Airflow
###############################################################################

airflow:
	docker compose up airflow

###############################################################################
# Spark
###############################################################################

spark:
	docker compose up spark

###############################################################################
# API
###############################################################################

api:
	uvicorn api.main:app --reload

###############################################################################
# Terraform
###############################################################################

terraform-init:
	cd terraform/environments/dev && terraform init

terraform-plan:
	cd terraform/environments/dev && terraform plan

terraform-apply:
	cd terraform/environments/dev && terraform apply

###############################################################################
# Full Platform Refresh
###############################################################################

full-refresh:
	python -m simulator.simulator
	python -m quality.validation
	python -m etl.extract
	python -m etl.bronze_loader
	python -m etl.silver_loader
	python -m etl.gold_loader
	cd dbt && dbt build