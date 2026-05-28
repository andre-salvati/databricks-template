import argparse
import os
import re
import tomllib
from pathlib import Path

import yaml
from databricks.bundles.jobs import (
    CronSchedule,
    Job,
    JobEmailNotifications,
    JobEnvironment,
    JobNotificationSettings,
    JobParameterDefinition,
    JobsHealthMetric,
    JobsHealthOperator,
    JobsHealthRule,
    JobsHealthRules,
    PipelineTask,
    PythonWheelTask,
    QueueSettings,
    RunJobTask,
    Task,
    TaskDependency,
)
from databricks.bundles.jobs._models.environment import Environment
from databricks.bundles.pipelines import (
    FileLibrary,
    Pipeline,
    PipelineLibrary,
    PipelinesEnvironment,
)
from databricks.sdk import WorkspaceClient

JOB_NAME = "job1"
PIPELINE_NAME = "job1_sdp"


def _dqx_package() -> str:
    """Read the DQX version from pyproject.toml so it stays in sync with the runtime dep."""
    pyproject = Path(__file__).resolve().parent.parent / "pyproject.toml"
    with open(pyproject, "rb") as f:
        deps = tomllib.load(f)["project"]["dependencies"]
    for dep in deps:
        if dep.startswith("databricks-labs-dqx"):
            return dep
    raise ValueError("databricks-labs-dqx not found in pyproject.toml dependencies")


DQX_PACKAGE = _dqx_package()

# Service principal name. "make init" overrides with parameter.
SP_DISPLAY_NAME = "template-sp"

# Used as the team / cost-center tag. Surfaces in system.billing.usage.
COST_CENTER = "data-platform"
TEAM = "data-engineering"

# Where prod failures are emailed. CI overrides via TEMPLATE_ALERT_EMAILS=a@x.com,b@y.com.
DEFAULT_ALERT_EMAILS = ["data-platform-oncall@example.com"]

# Job-level parameter defaults. Operators can override per-run from the Databricks
# Jobs UI "Run with different parameters" dialog without rewriting the full task params.
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_QUARANTINE_FAIL_RATIO = "1.0"  # disabled; all rows pass through in dev/staging
PROD_QUARANTINE_FAIL_RATIO = "0.1"  # hard-fail if >10% of rows quarantined in prod

# Trigger the on_duration_warning_threshold_exceeded email if the prod run takes
# longer than this. Nominal end-to-end is ~5 minutes, so 30 minutes leaves headroom
# for transient slowness without crying wolf.
DURATION_WARNING_SECONDS = 1800

# Per-task timeouts. A single hung task can't consume the whole job budget,
# and downstream tasks fail fast with a clear cause.
TIMEOUT_HEALTH_CHECK_S = 300
TIMEOUT_EXTRACT_S = 900
TIMEOUT_TRANSFORM_S = 1800
TIMEOUT_INTEGRATION_S = 900

# Back off before retrying so transient lock/metastore blips have time to clear.
MIN_RETRY_INTERVAL_MS = 60_000


def _project_version() -> str:
    """Read version from pyproject.toml so the bundle pins to the exact built wheel."""
    pyproject = Path(__file__).resolve().parent.parent / "pyproject.toml"
    with open(pyproject, "rb") as f:
        return tomllib.load(f)["project"]["version"]


WHEEL_GLOB = f"../dist/template-{_project_version()}-py3-none-any.whl"


def _get_service_principal_id(display_name: str, profile: str) -> int:
    workspace = WorkspaceClient(profile=profile)
    for sp in workspace.service_principals.list():
        if sp.display_name == display_name:
            return sp.application_id
    raise ValueError(f"Service principal '{display_name}' not found in workspace.")


def _wheel_task() -> PythonWheelTask:
    return PythonWheelTask(
        package_name="template",
        entry_point="main",
        parameters=[
            "--task={{task.name}}",
            "--env=${bundle.target}",
            "--run-id={{job.run_id}}",
            "--log-level={{job.parameters.log_level}}",
            "--quarantine-fail-ratio={{job.parameters.quarantine_fail_ratio}}",
            "--seed-date={{job.parameters.seed_date}}",
        ],
    )


def _environments() -> list[JobEnvironment]:
    return [
        JobEnvironment(
            environment_key="default",
            spec=Environment(client="5", dependencies=[WHEEL_GLOB]),
        )
    ]


def _tags(environment: str) -> dict[str, str]:
    return {
        "git_branch": "${bundle.git.branch}",
        "git_origin_url": "${bundle.git.origin_url}",
        "environment": environment,
        "cost_center": COST_CENTER,
        "team": TEAM,
    }


def _alert_emails() -> list[str]:
    raw = os.environ.get("TEMPLATE_ALERT_EMAILS")
    if raw:
        return [e.strip() for e in raw.split(",") if e.strip()]
    return DEFAULT_ALERT_EMAILS


def _retries(environment: str) -> int:
    # Be conservative in dev (faster feedback when something is wrong);
    # retry transient failures in higher envs.
    return 0 if environment == "dev" else 2


def _retry_kwargs(retries: int) -> dict:
    # Avoid setting min_retry_interval_millis when retries=0 (it would be wasted config);
    # always set retry_on_timeout=False so a 30min hung task isn't retried twice.
    if retries == 0:
        return {"max_retries": 0}
    return {
        "max_retries": retries,
        "min_retry_interval_millis": MIN_RETRY_INTERVAL_MS,
        "retry_on_timeout": False,
    }


def _target_overrides(environment: str, sp_id: str) -> dict:
    """Target-level run_as and root_path pinned to the SP for staging/prod.

    Two independent gates stack here:
      1. Target-level run_as makes mode: production's deployer-identity check
         actually enforce — without a target-level run_as, the check has nothing
         to compare against and silently falls through.
      2. SP-pinned root_path means the workspace ACL itself blocks non-SP
         deployers from writing under /Workspace/Users/<sp-uuid>/..., independent
         of mode: production.

    These overrides are merged into databricks.yml's `targets:` block via the
    bundle's `include: resources/*.yml`.
    """
    return {
        "run_as": {"service_principal_name": sp_id},
        "workspace": {"root_path": f"/Workspace/Users/{sp_id}/.bundle/{environment}/${{bundle.name}}"},
    }


def _build_job(environment: str, sp_id: str | None) -> dict:
    retries = _retries(environment)
    tasks: list[Task] = []

    # On prod we run a health check first so a broken bundle/wheel/grant fails the
    # smoke task instead of corrupting the medallion tables.
    if environment == "prod":
        tasks.append(
            Task(
                task_key="health_check",
                max_retries=1,
                min_retry_interval_millis=MIN_RETRY_INTERVAL_MS,
                timeout_seconds=TIMEOUT_HEALTH_CHECK_S,
                environment_key="default",
                python_wheel_task=_wheel_task(),
            )
        )

    # seed_sources runs only in prod: staging/dev use the integration test `setup` task
    # to seed external_source with controlled data, so seed_sources would add noise there.
    # In prod, seed_sources runs after health_check and before the extract tasks.
    if environment == "prod":
        tasks.append(
            Task(
                task_key="seed_sources",
                **_retry_kwargs(retries),
                timeout_seconds=TIMEOUT_EXTRACT_S,
                environment_key="default",
                depends_on=[TaskDependency(task_key="health_check")],
                python_wheel_task=_wheel_task(),
            )
        )

    extract_deps: list[TaskDependency] = [TaskDependency(task_key="seed_sources")] if environment == "prod" else []

    tasks.extend(
        [
            Task(
                task_key="extract_source1",
                **_retry_kwargs(retries),
                timeout_seconds=TIMEOUT_EXTRACT_S,
                environment_key="default",
                depends_on=extract_deps or None,
                python_wheel_task=_wheel_task(),
            ),
            Task(
                task_key="extract_source2",
                **_retry_kwargs(retries),
                timeout_seconds=TIMEOUT_EXTRACT_S,
                environment_key="default",
                depends_on=extract_deps or None,
                python_wheel_task=_wheel_task(),
            ),
            Task(
                task_key="generate_orders",
                **_retry_kwargs(retries),
                timeout_seconds=TIMEOUT_TRANSFORM_S,
                environment_key="default",
                depends_on=[
                    TaskDependency(task_key="extract_source1"),
                    TaskDependency(task_key="extract_source2"),
                ],
                python_wheel_task=_wheel_task(),
            ),
            Task(
                task_key="generate_orders_agg",
                **_retry_kwargs(retries),
                timeout_seconds=TIMEOUT_TRANSFORM_S,
                environment_key="default",
                depends_on=[TaskDependency(task_key="generate_orders")],
                python_wheel_task=_wheel_task(),
            ),
        ]
    )

    schedule = None
    email_notifications = None
    health = None
    if environment == "prod":
        schedule = CronSchedule(quartz_cron_expression="0 0 5 * * ?", timezone_id="UTC")
        email_notifications = JobEmailNotifications(
            on_failure=_alert_emails(),
            on_duration_warning_threshold_exceeded=_alert_emails(),
        )
        # Without this rule, on_duration_warning_threshold_exceeded above
        # would be wired to an event that can never fire.
        health = JobsHealthRules(
            rules=[
                JobsHealthRule(
                    metric=JobsHealthMetric.RUN_DURATION_SECONDS,
                    op=JobsHealthOperator.GREATER_THAN,
                    value=DURATION_WARNING_SECONDS,
                )
            ]
        )

    job = Job(
        name=f"{JOB_NAME}_${{bundle.target}}",
        timeout_seconds=3600,
        # max_concurrent_runs=1 + queue.enabled=True: if a run is already in flight
        # when the next scheduled tick arrives, queue it rather than silently
        # skipping. Skipping a scheduled prod run is almost never what you want.
        max_concurrent_runs=1,
        queue=QueueSettings(enabled=True),
        # Suppress alerts for runs that were manually cancelled or skipped by an
        # upstream condition — those aren't failures and shouldn't page anyone.
        notification_settings=JobNotificationSettings(
            no_alert_for_canceled_runs=True,
            no_alert_for_skipped_runs=True,
        ),
        parameters=[
            JobParameterDefinition(name="log_level", default=DEFAULT_LOG_LEVEL),
            JobParameterDefinition(
                name="quarantine_fail_ratio",
                default=PROD_QUARANTINE_FAIL_RATIO if environment == "prod" else DEFAULT_QUARANTINE_FAIL_RATIO,
            ),
            # Empty string → SeedSources resolves to today's date at runtime.
            # Override per-run (e.g. "2024-03-15") to backfill a specific day.
            JobParameterDefinition(name="seed_date", default=""),
        ],
        tags=_tags(environment),
        environments=_environments(),
        schedule=schedule,
        email_notifications=email_notifications,
        health=health,
        tasks=tasks,
    )

    d = job.as_dict()
    d["deployment"] = {"kind": "BUNDLE"}

    if environment in ("staging", "prod"):
        # Pin the run-as identity to the SP application id rather than $current_user
        # so a developer running `make deploy env=staging` doesn't accidentally deploy
        # a job that runs as themselves. Same SP is also wired at the target level by
        # _target_overrides() so mode: production can actually enforce identity.
        d["run_as"] = {"service_principal_name": sp_id}
        d["permissions"] = [{"service_principal_name": sp_id, "level": "CAN_MANAGE"}]

    return d


def _build_job_integration_test(environment: str, sp_id: str | None) -> dict:
    """Integration test: setup → run (job1 batch) + run_sdp (pipeline) in parallel → validate.

    A single validate task waits for both run and run_sdp to finish, then checks
    both the batch output (report.order_agg) and the SDP output (report.order_agg_sdp).
    """
    tasks = [
        Task(
            task_key="setup",
            max_retries=0,
            timeout_seconds=TIMEOUT_INTEGRATION_S,
            environment_key="default",
            python_wheel_task=_wheel_task(),
        ),
        Task(
            task_key="run",
            depends_on=[TaskDependency(task_key="setup")],
            run_job_task=RunJobTask(job_id=f"${{{f'resources.jobs.{JOB_NAME}.id'}}}"),
        ),
        Task(
            task_key="run_sdp",
            depends_on=[TaskDependency(task_key="setup")],
            pipeline_task=PipelineTask(
                pipeline_id="${resources.pipelines.job1_sdp.id}",
                full_refresh=True,
            ),
        ),
        Task(
            task_key="validate",
            max_retries=0,
            timeout_seconds=TIMEOUT_INTEGRATION_S,
            environment_key="default",
            depends_on=[TaskDependency(task_key="run"), TaskDependency(task_key="run_sdp")],
            python_wheel_task=_wheel_task(),
        ),
    ]

    job = Job(
        name=f"{JOB_NAME}_${{bundle.target}}_integration_test",
        timeout_seconds=3600,
        max_concurrent_runs=1,
        queue=QueueSettings(enabled=True),
        parameters=[
            JobParameterDefinition(name="log_level", default=DEFAULT_LOG_LEVEL),
            JobParameterDefinition(name="quarantine_fail_ratio", default=DEFAULT_QUARANTINE_FAIL_RATIO),
            JobParameterDefinition(name="seed_date", default=""),
        ],
        tags=_tags(environment),
        environments=_environments(),
        tasks=tasks,
    )

    d = job.as_dict()
    d["deployment"] = {"kind": "BUNDLE"}

    if environment in ("staging", "prod"):
        d["run_as"] = {"service_principal_name": sp_id}
        d["permissions"] = [{"service_principal_name": sp_id, "level": "CAN_MANAGE"}]

    return d


def _resolve_catalog(environment: str) -> str:
    """Compute the target catalog at generation time, mirroring Config.__init__.

    - dev      → dev_<sanitized_user>  (per-developer sandbox)
    - staging / prod → environment name
    """
    if environment == "dev":
        ws = WorkspaceClient(profile="dev")
        local_part = ws.current_user.me().user_name.split("@")[0]
        user = re.sub(r"[^a-zA-Z0-9_]", "_", local_part)
        return f"dev_{user}"
    return environment


def _build_pipeline(environment: str, catalog: str, sp_id: str | None) -> dict:
    pipeline = Pipeline(
        name=f"{PIPELINE_NAME}_${{bundle.target}}",
        serverless=True,
        catalog=catalog,
        schema="raw",
        development=(environment == "dev"),
        libraries=[PipelineLibrary(file=FileLibrary(path="../src/template/job1_sdp/pipeline.py"))],
        environment=PipelinesEnvironment(dependencies=[WHEEL_GLOB]),
        configuration={"target_catalog": catalog},
        tags=_tags(environment),
    )

    d = pipeline.as_dict()

    if environment in ("staging", "prod"):
        d["run_as"] = {"service_principal_name": sp_id}
        d["permissions"] = [{"service_principal_name": sp_id, "level": "IS_OWNER"}]

    return d


def main():
    parser = argparse.ArgumentParser(description="Generate Databricks Jobs YAML")
    parser.add_argument("environment", choices=["dev", "staging", "prod"])
    args = parser.parse_args()

    env = args.environment

    # Resolve SP app id once for staging/prod; reused for job-level run_as/permissions
    # and target-level run_as/root_path so the SP UUID lives in exactly one place.
    sp_id: str | None = None
    if env in ("staging", "prod"):
        sp_id = os.environ.get("TEMPLATE_SP_APP_ID") or _get_service_principal_id(SP_DISPLAY_NAME, profile=env)

    catalog = _resolve_catalog(env)

    jobs: dict = {JOB_NAME: _build_job(env, sp_id)}
    if env in ("dev", "staging"):
        jobs[f"{JOB_NAME}_integration_test"] = _build_job_integration_test(env, sp_id)

    output: dict = {
        "resources": {
            "jobs": jobs,
            "pipelines": {PIPELINE_NAME: _build_pipeline(env, catalog, sp_id)},
        }
    }
    if sp_id is not None:
        output["targets"] = {env: _target_overrides(env, sp_id)}

    output_file = "./resources/jobs.yml"
    with open(output_file, "w") as f:
        yaml.dump(output, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

    print(f"Generated {output_file}")


if __name__ == "__main__":
    main()
