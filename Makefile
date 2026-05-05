env ?= dev

sync:
	uv sync --all-extras

test:
	uv run pytest

pre-commit:
	pre-commit autoupdate
	pre-commit run --all-files

init:
	uv run python ./scripts/sdk_init_workspace.py --storage-root s3://your-s3-bucket

deploy:
	uv run python ./scripts/sdk_generate_template_job.py $(env)
	uv run databricks bundle deploy --target $(env)

run:
	uv run databricks bundle run job1_dev_integration_test --target $(env)

