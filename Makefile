# =============================================================================
# StreamFlow Analytics — Makefile
# One-command task runner for all common operations
# =============================================================================

.PHONY: help bootstrap check generate-data run-pipeline run-step \
        test test-unit test-integration test-e2e coverage \
        dq-check dbt-run dbt-test dbt-docs \
        docker-up docker-down \
        tf-plan tf-apply lint format clean

# Defaults
START    ?= 2024-01-01
END      ?= 2024-12-31
COMPANIES ?= 50000
ENV      ?= local
STEP     ?= 1
TABLE    ?= rpt_cancel_flow_final_metrics
PYTHON   ?= python

help:  ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-28s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "  Usage: make <target> [START=2024-01-01] [END=2024-12-31] [ENV=local]"

# =============================================================================
# Setup
# =============================================================================

bootstrap: ## Create venv, install deps, setup pre-commit hooks
	@echo "🚀 Bootstrapping StreamFlow Analytics..."
	python -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -r requirements.txt
	.venv/bin/pip install -e .
	.venv/bin/pre-commit install
	@echo "✅ Bootstrap complete. Activate: source .venv/bin/activate"

check: ## Verify installation
	$(PYTHON) -c "import pyspark; print('PySpark:', pyspark.__version__)"
	$(PYTHON) -c "import delta; print('Delta Lake: OK')"
	$(PYTHON) -c "import mlflow; print('MLflow:', mlflow.__version__)"
	$(PYTHON) -c "import great_expectations; print('GE: OK')"

# =============================================================================
# Data Generation
# =============================================================================

generate-data: ## Generate synthetic cancel-flow data
	@echo "🏗️  Generating synthetic data: $(START) → $(END), $(COMPANIES) companies"
	$(PYTHON) data/synthetic/generator.py \
		--start-date $(START) \
		--end-date $(END) \
		--companies $(COMPANIES) \
		--output-path ./data/raw

# =============================================================================
# Pipeline Execution
# =============================================================================

run-pipeline: ## Run full pipeline (Steps 0-4 + DQ + OPTIMIZE)
	@echo "▶ Running full pipeline: $(START) → $(END) [$(ENV)]"
	./scripts/run_pipeline.sh \
		--start-date $(START) \
		--end-date $(END) \
		--env $(ENV)

run-step: ## Run single pipeline step (STEP=1)
	@echo "▶ Running Step $(STEP): $(START) → $(END) [$(ENV)]"
	./scripts/run_step.sh \
		--step $(STEP) \
		--start-date $(START) \
		--end-date $(END) \
		--env $(ENV)

backfill: ## Historical backfill (START=2024-01-01 END=2024-12-31)
	@echo "📚 Running backfill: $(START) → $(END)"
	./scripts/backfill.sh \
		--start-date $(START) \
		--end-date $(END) \
		--env $(ENV)

# =============================================================================
# Testing
# =============================================================================

test: ## Run all tests
	$(PYTHON) -m pytest tests/ -v --tb=short -q

test-unit: ## Run unit tests only (fast, no Spark)
	$(PYTHON) -m pytest tests/unit/ -v --tb=short

test-integration: ## Run integration tests (PySpark required)
	$(PYTHON) -m pytest tests/integration/ -v --tb=short -q

test-e2e: ## Run end-to-end pipeline test
	$(PYTHON) -m pytest tests/e2e/ -v --tb=short -s

coverage: ## Run tests with coverage report
	$(PYTHON) -m pytest tests/ \
		--cov=src \
		--cov-report=term-missing \
		--cov-report=html:htmlcov \
		--cov-fail-under=90
	@echo "📊 Coverage report: htmlcov/index.html"

# =============================================================================
# Data Quality
# =============================================================================

dq-check: ## Run DQ checks (TABLE=rpt_cancel_flow_final_metrics)
	$(PYTHON) src/dq/dq_checks.py \
		--table $(TABLE) \
		--data-path ./data/output

dq-check-all: ## Run DQ checks on ALL tables
	$(PYTHON) src/dq/dq_checks.py \
		--all \
		--data-path ./data/output

ge-checkpoint: ## Run Great Expectations checkpoint
	great_expectations checkpoint run cancel_flow_checkpoint

# =============================================================================
# dbt
# =============================================================================

dbt-run: ## Run all dbt models
	cd dbt && dbt run --profiles-dir .

dbt-test: ## Run dbt tests
	cd dbt && dbt test --profiles-dir .

dbt-docs: ## Generate and serve dbt docs
	cd dbt && dbt docs generate --profiles-dir . && dbt docs serve

dbt-lineage: ## Show dbt lineage graph
	cd dbt && dbt ls --select "+rpt_cancel_flow_final_metrics+"

# =============================================================================
# Local Services (Docker)
# =============================================================================

docker-up: ## Start local stack (Airflow + MLflow + Grafana)
	docker-compose up -d
	@echo ""
	@echo "✅ Services running:"
	@echo "   Airflow:  http://localhost:8080  (admin/admin)"
	@echo "   MLflow:   http://localhost:5000"
	@echo "   Grafana:  http://localhost:3000  (admin/admin)"
	@echo "   API:      http://localhost:8000/docs"

docker-down: ## Stop local stack
	docker-compose down

api-start: ## Start FastAPI server locally
	uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000

# =============================================================================
# Infrastructure
# =============================================================================

tf-init: ## Terraform init (ENV=prod)
	terraform -chdir=infrastructure/terraform/environments/$(ENV) init

tf-plan: ## Terraform plan
	terraform -chdir=infrastructure/terraform/environments/$(ENV) plan

tf-apply: ## Terraform apply (requires confirmation)
	terraform -chdir=infrastructure/terraform/environments/$(ENV) apply

tf-destroy: ## Terraform destroy (DANGEROUS)
	@read -p "⚠️  Destroy ALL resources in $(ENV)? [yes/no] " confirm; \
	[ "$$confirm" = "yes" ] && \
	terraform -chdir=infrastructure/terraform/environments/$(ENV) destroy || \
	echo "Aborted."

# =============================================================================
# Code Quality
# =============================================================================

lint: ## Run flake8 + mypy
	$(PYTHON) -m flake8 src/ tests/ --max-line-length=120 --ignore=E501,W503
	$(PYTHON) -m mypy src/ --ignore-missing-imports

format: ## Auto-format with black + isort
	$(PYTHON) -m black src/ tests/ data/ dbt/
	$(PYTHON) -m isort src/ tests/ data/ dbt/

pre-commit: ## Run pre-commit hooks on all files
	pre-commit run --all-files

# =============================================================================
# Cleanup
# =============================================================================

clean: ## Remove generated artifacts
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -name "*.pyc" -delete
	find . -name ".pytest_cache" -exec rm -rf {} +
	rm -rf htmlcov/ .coverage dist/ build/ *.egg-info/
	@echo "✅ Clean complete"
