env ?= dev

sync:
	uv sync --all-extras

test:
	uv run pytest

pre-commit:
	pre-commit autoupdate
	pre-commit run --all-files

deploy:
	uv run python ./scripts/generate_template_workflow.py $(env)
	uv run databricks bundle deploy --target $(env)

deploy-serverless:
	uv run python ./scripts/generate_template_workflow.py $(env) --serverless
	uv run databricks bundle deploy --target $(env)

run:
	uv run databricks bundle run integration_test_job --target $(env)

