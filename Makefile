env ?= dev
yes ?=

sync:
	uv sync --all-extras

unit-test:
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

drop: whoami
	uv run python ./scripts/sdk_drop_tables.py $(env) $(yes)

project-costs: aws-profile ?= costs
project-costs:
	uv run python ./scripts/project_costs.py $(if $(aws-profile),--aws-profile $(aws-profile),)

# Diagram a SQL query with sqlglot, writing .mmd + .svg into reports/sql-diagram/. Pass the query
# with sql=path/to/query.sql (stdin if omitted); optionally name=basename, mode=plan|lineage
# (default plan: the query's steps) and comments=1 (looks up each table's Unity Catalog comment).
sql-diagram:
	uv run python ./scripts/sql_diagram.py $(if $(sql),--file $(sql),) $(if $(name),--name $(name),) \
		$(if $(mode),--mode $(mode),) $(if $(comments),--comments,)

# Regenerate the README star-history chart from the GitHub API. The SVGs are committed,
# so the README renders from this repo rather than a third-party chart service.
star-history:
	uv run python ./scripts/star_history.py
