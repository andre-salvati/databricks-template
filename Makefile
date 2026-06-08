env ?= dev
yes ?=

sync:
	uv sync --all-extras

test:
	uv run pytest

pre-commit:
	pre-commit autoupdate
	pre-commit run --all-files

init:
	uv run python ./scripts/sdk_init_workspace.py --storage-root s3://your-s3-bucket

# Print the identity that the $(env) profile authenticates as. Used as a guardrail
# before deploy/run so it's obvious whether you're acting as the SP or as your own
# user — especially on staging/prod, where the wrong identity can silently bypass
# (or trip) the mode: production deployer-identity check.
whoami:
	@echo "==> Target: $(env)"
	@uv run databricks current-user me --profile $(env) | \
		python3 -c "import sys, json; d = json.load(sys.stdin); print('==> Identity:', (d.get('displayName') or d.get('userName')), '(' + d.get('userName', '?') + ')')"

deploy: whoami
	uv run python ./scripts/sdk_generate_template_job.py $(env)
	uv run databricks bundle deploy --target $(env)

run: whoami
	uv run databricks bundle run job1_integration_test --target $(env)

truncate: whoami
	uv run python ./scripts/sdk_truncate_tables.py $(env) $(yes)

project-costs:
	uv run python ./scripts/project_costs.py
