env ?= dev

sync:
	uv sync --all-extras

test:
	uv run pytest

pre-commit:
	pre-commit autoupdate
	pre-commit run --all-files

create-sp:
	uv run python ./scripts/sdk_create_sp.py template-sp

deploy:
	uv run python ./scripts/sdk_generate_template_job.py $(env)
	uv run databricks bundle deploy --target $(env)

run:
	uv run databricks bundle run integration_test_job --target $(env)

