# databricks-template

> A production-ready PySpark project template with medallion architecture, Python packaging, unit tests, integration tests, coverage tests, CI/CD automation, Declarative Automation Bundles, and DQX data quality framework.

![Databricks](https://img.shields.io/badge/platform-Databricks-orange?logo=databricks)
![PySpark](https://img.shields.io/badge/pyspark-4.1+-brightgreen?logo=apache-spark)
![CI/CD](https://img.shields.io/github/actions/workflow/status/andre-salvati/databricks-template/.github/workflows/onpush.yml)
![Stars](https://img.shields.io/github/stars/andre-salvati/databricks-template?style=social)

## 🚀 Overview

This project template is designed to boost productivity and promote maintainability when developing ETL pipelines on Databricks. It aims to bring software engineering best practices—such as modular architecture, automated unit and integration testing, and CI/CD—into the world of data engineering. By combining a clean project structure with robust development and deployment jobs, this template helps teams move faster with confidence.

You’re encouraged to adapt the structure and tooling to suit your project’s specific needs and environment.

Interested in bringing these principles in your own project?  Let’s [connect on Linkedin](https://www.linkedin.com/in/andresalvati/).

## 🧪 Technologies

- Databricks Free Edition (Serverless)
- Databricks Runtime 18.0 LTS
- Databricks Unity Catalog
- Databricks Declarative Automation Bundles (former Databricks Asset Bundles)
- Databricks CLI
- Databricks Python SDK
- Databricks DQX
- Databricks AI Dev Kit
- Claude Code
- PySpark 4.1
- Python 3.12+
- GitHub Actions
- Pytest

## 📦 Features

This project template demonstrates how to:

- use agentic development (with Databricks AI Dev Kit and Claude Code) in data projects.
- structure PySpark code inside classes/packages, instead of notebooks.
- package and deploy code to different environments (dev, staging, prod). 
- use a CI/CD pipeline with [Github Actions](https://docs.github.com/en/actions).
- run unit tests on transformations with [pytest package](https://pypi.org/project/pytest/). Set up VSCode to run unit tests on your local machine.
- run integration tests setting the input data and validating the output data.
- isolate "dev" environments / catalogs to avoid concurrency issues between developer tests.
- show developer name and branch as job tags to track issues.
- utilize [coverage package](https://pypi.org/project/coverage/) to generate test coverage reports.
- utilize [uv](https://docs.astral.sh/uv/) as a project/package manager.
- configure job to run tasks selectively.
- use [medallion architecture](https://www.databricks.com/glossary/medallion-architecture) pattern.
- lint and format code with [ruff](https://docs.astral.sh/ruff/) and [pre-commit](https://pre-commit.com/).
- use a Make file to automate repetitive tasks.
- utilize [argparse package](https://pypi.org/project/argparse/) to build a flexible command line interface to start the jobs.

<br>

- utilize [Databricks Declarative Automation Bundles](https://docs.databricks.com/en/dev-tools/bundles/index.html) to package/deploy/run a Python wheel package on Databricks.
- configure jobs to run across multiple environments by generating environment-specific job definitions using the [Databricks SDK](https://docs.databricks.com/aws/en/dev-tools/sdk-python#create-a-job-that-uses-serverless-compute).
- utilize [Databricks DQX](https://databrickslabs.github.io/dqx/) to define and enforce data quality rules, such as null checks, uniqueness, thresholds, and schema validation, and filter bad data on quarantine tables.
- utilize [service principals](https://docs.databricks.com/aws/en/admin/users-groups/service-principals) to run production code
- utilize [Databricks SDK for Python](https://docs.databricks.com/en/dev-tools/sdk-python.html) to manage workspaces and accounts and analyse costs. Refer to 'scripts' folder for some examples. 
- utilize [Databricks Unity Catalog](https://www.databricks.com/product/unity-catalog) and get data lineage for your tables and columns.
- utilize [Databricks Lakeflow Jobs](https://docs.databricks.com/en/workflows/index.html) to execute a DAG and [task parameters](https://docs.databricks.com/en/workflows/jobs/parameter-value-references.html) to share context information between tasks (see [Task Parameters section](#task-parameters)). Yes, you don't need Airflow to manage your DAGs here!!!
- utilize serverless job clusters on [Databricks Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition) to deploy your pipelines.

## 🧠 Resources

Agentic development:
- [Claude Code: 5 Essentials for Data Engineering](https://www.youtube.com/watch?v=YnIWW88l0mc)
- [Mastering Claude Code in 30 minutes](https://www.youtube.com/watch?v=6eBSHbLKuN0)
- [Introducing Databricks AI Dev Kit - Skills, MCP server, Builder App](https://www.youtube.com/watch?v=HFSIKrG8bRg)

Debates on the use of notebooks vs. Python packaging:
- [The Rise of The Notebook Engineer](https://dataengineeringcentral.substack.com/p/the-rise-of-the-notebook-engineer)
- [Please don’t make me use Databricks notebooks](https://medium.com/@seade03/please-dont-make-me-use-databricks-notebooks-3d07a4a332ae)
- [this Linkedin thread by Daniel Beach](https://www.linkedin.com/posts/daniel-beach-6ab8b4132_dataengineering-databricks-activity-7171661784997715968-OpRW)
- [this Linkedin thread by Ryan Chynoweth](https://www.linkedin.com/posts/ryan-chynoweth_using-databricks-notebooks-for-production-activity-7170868557621186561-eo3P)
- [this Linkedin thread by Jaco van Gelder](https://www.linkedin.com/posts/jwvangelder_my-honest-opinion-on-notebooks-vs-python-activity-7385955500007534592-xwHa/)

Sessions on Databricks Declarative Automation Bundles, CI/CD, and Software Development Life Cycle at Data + AI Summit 2025:
- [CI/CD for Databricks: Advanced Asset Bundles and GitHub Actions](https://www.youtube.com/watch?v=XumUXF1e6RI)
- [Deploying Databricks Asset Bundles (DABs) at Scale](https://www.youtube.com/watch?v=mMwprgB-sIU)
- [A Prescription for Success: Leveraging DABs for Faster Deployment and Better Patient Outcomes](https://www.youtube.com/watch?v=01JHTM2UP-U)

Other resources:
- [Goodbye Pip and Poetry. Why UV Might Be All You Need](https://codecut.ai/why-uv-might-all-you-need/)
- [The Spark Revolution You Didn’t See Coming: How Apache Spark 4.0 in Databricks Just Changed Everything](https://medium.com/@matiasmaquieira96/the-spark-revolution-you-didnt-see-coming-how-apache-spark-4-0-2a6422144f67)



## 📁 Folder Structure

```
databricks-template/
│
├── .github/                       # CI/CD automation
│   └── workflows/
│       └── onpush.yml             # GitHub Actions pipeline
│
├── src/                           # Main source code
│   └── template/                  # Python package
│       ├── main.py                # Entry point with CLI (argparse)
│       ├── config.py              # Configuration management
│       ├── baseTask.py            # Base class for all tasks
│       ├── commonSchemas.py       # Shared PySpark schemas
│       ├── job1/                  # Job-specific tasks
│       │   ├── extract_source1.py
│       │   ├── extract_source2.py        # DQX validation + quarantine
│       │   ├── generate_orders.py
│       │   ├── generate_orders_agg.py
│       │   ├── health_check.py           # Prod smoke task (runs first)
│       │   ├── integration_setup.py
│       │   └── integration_validate.py
│       └── job2/                  # Additional job tasks
│
├── tests/                          # Unit tests
│   ├── job1/
│   │   └── unit_test.py            # Pytest unit tests
│   └── job2/
│
├── resources/                      # Databricks workflow templates
│   └── jobs.yml                    # Generated job definition (auto-created)
│
├── scripts/                              # Helper scripts
│   ├── sdk_generate_template_job.py      # Job definition generator (Databricks SDK)
│   ├── sdk_init_workspace.py             # Workspace initialization (SP, catalogs, schemas, grants)
│   ├── sdk_analyze_job_costs.py          # Cost analysis script
│   └── sdk_workspace_and_account.py      # Workspace and account management
│
├── docs/                           # Documentation assets
│   ├── dag.png
│   ├── task_output.png
│   ├── data_lineage.png
│   ├── data_quality.png
│   └── ci_cd.png
│
├── dist/                        # Build artifacts (Python wheel)
├── coverage_reports/            # Test coverage reports
│
├── databricks.yml               # Declarative Automation Bundle config
├── pyproject.toml               # Python project configuration (uv)
├── Makefile                     # Build automation
├── .pre-commit-config.yaml      # Pre-commit hooks (ruff)
└── README.md                    # This file
```

## CI/CD pipeline

<br>

<img src="docs/ci_cd.png">

<br>

## Databricks Jobs

<br>

<img src="docs/dag.png">

<br>

## Task Output

<br>

<img src="docs/task_output.png">

<br>

## Data Lineage (Unity Catalog)

<br>


<img src="docs/data_lineage.png">

<br>

## Quarantine table (generated by Databricks DQX)

<br>

<img src="docs/data_quality.png">

<br>


## Instructions


1) (Optional) Install [Databricks AI Dev Kit](https://github.com/databricks-solutions/ai-dev-kit) and [Claude Code](https://code.claude.com/docs/en/vs-code).

2) Create a workspace. Use a [Databricks Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition) workspace.


3) Install and configure Databricks CLI on your local machine. Check the current version on databricks.yaml. Follow instructions [here](https://docs.databricks.com/en/dev-tools/cli/install.html). 


4) Build Python env and execute unit tests on your local machine.

        make sync & make test
        
5) Create an external location in Databricks and update the "storage-root" parameter in the Makefile. This step will create the catalogs, schemas, service principal, and the required grants. For more details, see [Overview of external locations](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage#external-locations). Then run:

        make init

   If your workspace has more than one SQL warehouse, you'll need to disambiguate via `--warehouse-name`:

        uv run python ./scripts/sdk_init_workspace.py --storage-root s3://your-bucket --warehouse-name "Serverless Starter Warehouse"

6) Generate a secret for the service principal. In Databricks, go to: Workspace -> Settings -> Identity and access -> Service principals -> Secrets. Generate a new secret for your service principal and update the corresponding profiles in your .databrickscfg file. Your configuration should look similar to this:

        [dev]
        host             = https://xxxx.cloud.databricks.com/
        token            = bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
                        
        [staging]
        host          = https://xxxx.cloud.databricks.com/
        client_id     = yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy
        client_secret = aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa

        [prod]
        host          = https://xxxx.cloud.databricks.com/
        client_id     = yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy
        client_secret = aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa

7) Deploy and execute on the dev workspace.

        make deploy env=dev


8) Configure CI/CD automation with the service principal ID and Secret. Configure [Github Actions repository secrets](https://docs.github.com/en/actions/security-guides/using-secrets-in-github-actions) (DATABRICKS_HOST, DATABRICKS_PRINCIPAL_ID, DATABRICKS_SECRET).

9) (Optional) You can also execute unit tests from your preferred IDE. Here's a screenshot from [VS Code](https://code.visualstudio.com/) with [Microsoft's Python extension](https://marketplace.visualstudio.com/items?itemName=ms-python.python) installed.

- <img src="docs/vscode.png">


## Task parameters

The wheel entry point exposes a small, focused CLI. Most behavior is driven by `--env`; the rest is configuration.

- **`--task`** *(required)* — task to execute (`extract_source1`, `extract_source2`, `generate_orders`, `generate_orders_agg`, `setup`, `validate`, `health_check`). In jobs, this is auto-filled from `{{task.name}}`.
- **`--env`** *(required)* — deployment target (`dev`, `staging`, `prod`). Drives catalog selection (`dev_{user}` vs `staging` vs `prod`), retry counts, schedule, alerts, and whether the workspace SDK is real or mocked. In jobs, auto-filled from `${bundle.target}`.
- **`--skip`** *(optional)* — short-circuit the current task. Useful when paired with the `ops.config` skip table for tactical pipeline freezes.
- **`--run-id`** *(optional, observability-only)* — Databricks job run ID. Auto-filled from `{{job.run_id}}`; stamped onto every log line via a `logging.Filter` so production logs are correlatable after ingest into Splunk/Datadog/CloudWatch.

### What is *not* a CLI parameter (and why)

- **User identity** — derived at runtime from `WorkspaceClient().current_user.me()`, sanitized to a valid SQL identifier. Don't pass it.
- **Schema (medallion layer)** — each task hardcodes its read/write contract (e.g. `raw.customer` → `curated.order_enriched`). Layer names are architecture, not configuration.
- **Debug flag** — replaced by the `TEMPLATE_LOG_LEVEL` env var (see below).

## Runtime environment variables

These let you tune behavior without editing code or redeploying.

| Variable | Purpose | Default |
|---|---|---|
| `TEMPLATE_LOG_LEVEL` | `DEBUG` / `INFO` / `WARNING` etc. Set from the Databricks Jobs UI ("Run now with different parameters" → env vars) to bump verbosity for a single run during prod incident response. | `INFO` |
| `TEMPLATE_QUARANTINE_FAIL_RATIO` | Hard-fail `extract_source2` if more than this fraction of rows are quarantined by DQX. Set to e.g. `0.1` on prod to enforce; defaults to disabled so the demo seed data still ingests. | `1.0` |
| `TEMPLATE_ALERT_EMAILS` | Comma-separated recipients for prod `JobEmailNotifications` (on_failure + on_duration_warning). Wired from CI secret of the same name. | `data-platform-oncall@example.com` |
| `TEMPLATE_SP_APP_ID` | Override the service principal `application_id` looked up by display name. Used by CI to avoid the SCIM lookup. | resolved from `SP_DISPLAY_NAME` |

## Catalog & schema model

- **`dev_{sanitized_username}`** catalogs (one per developer) are created lazily by the developer on first run. Isolates concurrent feature work.
- **`staging`** and **`prod`** catalogs are provisioned upfront by `make init` (via `scripts/sdk_init_workspace.py`); runtime jobs in those environments must **not** have `CREATE CATALOG` privilege.
- Every catalog hosts the same fixed medallion schemas: `external_source`, `raw`, `curated`, `report`, and `ops` (internal — runtime config like the skip table; renamed from `system` to avoid colliding with Unity Catalog's reserved `system` catalog).

## Production guardrails

- **`databricks.yml`** sets `mode: production` on the prod target — DABs enforces that the deployer identity equals the run-as identity (the SP). `make deploy env=prod` from a developer's local machine will fail by design; only CI can push prod.
- **`run_as` and `permissions`** on every staging/prod job are pinned to the service principal's `application_id` (not `${workspace.current_user.userName}`), wired by `scripts/sdk_generate_template_job.py`.
- **`health_check` task** runs first in prod and fails fast on a broken wheel, missing grant, or unreachable SQL warehouse — before any medallion table is touched.
- **Wheel version pinning**: `_project_version()` reads `pyproject.toml` to produce the exact wheel filename in the bundle's `JobEnvironment.dependencies`, so a forgotten rebuild can't silently deploy an old wheel.
- **Per-environment retries**: 0 in dev (fast feedback), 2 in staging/prod (transient failure resilience). Retries on staging/prod back off `MIN_RETRY_INTERVAL_MS` (60s) before re-attempting, giving transient lock/metastore blips time to clear.
- **Per-task timeouts**: each task has its own `timeout_seconds` (300s for health-check, 900s for extracts, 1800s for transforms) so a single hung task can't consume the whole job budget.
- **Schema-drift guard**: all writes use `overwriteSchema=false` so an upstream change in column type or order fails the task loudly instead of silently propagating bad data.
- **Queued runs, not skipped**: prod has `max_concurrent_runs=1` paired with `queue.enabled=true` — if a run is still in flight when the next 5 a.m. tick arrives, the new run queues rather than getting silently dropped.
- **Health-rule-backed duration alert**: the `on_duration_warning_threshold_exceeded` email is wired to a `JobsHealthRule` on `RUN_DURATION_SECONDS > 1800` (30 min). Without that rule, the email would be wired to an event that can never fire.
- **Cancelled/skipped runs don't page**: `notification_settings.no_alert_for_canceled_runs` and `no_alert_for_skipped_runs` are both `true`, so manual cancellations or upstream-condition skips don't generate failure alerts.


## Star History

<a href="https://www.star-history.com/?repos=andre-salvati%2Fdatabricks-template&type=date&legend=top-left">
 <picture>
   <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/chart?repos=andre-salvati/databricks-template&type=date&theme=dark&legend=top-left" />
   <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/chart?repos=andre-salvati/databricks-template&type=date&legend=top-left" />
   <img alt="Star History Chart" src="https://api.star-history.com/chart?repos=andre-salvati/databricks-template&type=date&legend=top-left" />
 </picture>
</a>
