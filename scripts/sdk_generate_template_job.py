import argparse
import os
import tomllib
from pathlib import Path

import yaml
from databricks.bundles.jobs import (
    CronSchedule,
    Job,
    JobEmailNotifications,
    JobEnvironment,
    PythonWheelTask,
    RunJobTask,
    Task,
    TaskDependency,
)
from databricks.bundles.jobs._models.environment import Environment
from databricks.sdk import WorkspaceClient

JOB_NAME = "job1"

# Service principal name. "make init" overrides with parameter.
SP_DISPLAY_NAME = "template-sp"

# Used as the team / cost-center tag. Surfaces in system.billing.usage.
COST_CENTER = "data-platform"
TEAM = "data-engineering"

# Where prod failures are emailed. CI overrides via TEMPLATE_ALERT_EMAILS=a@x.com,b@y.com.
DEFAULT_ALERT_EMAILS = ["data-platform-oncall@example.com"]


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


def _build_job(environment: str) -> dict:
    retries = _retries(environment)
    tasks: list[Task] = []

    # On prod we run a health check first so a broken bundle/wheel/grant fails the
    # smoke task instead of corrupting the medallion tables.
    if environment == "prod":
        tasks.append(
            Task(
                task_key="health_check",
                max_retries=1,
                environment_key="default",
                python_wheel_task=_wheel_task(),
            )
        )

    extract_deps: list[TaskDependency] = [TaskDependency(task_key="health_check")] if environment == "prod" else []

    tasks.extend(
        [
            Task(
                task_key="extract_source1",
                max_retries=retries,
                environment_key="default",
                depends_on=extract_deps or None,
                python_wheel_task=_wheel_task(),
            ),
            Task(
                task_key="extract_source2",
                max_retries=retries,
                environment_key="default",
                depends_on=extract_deps or None,
                python_wheel_task=_wheel_task(),
            ),
            Task(
                task_key="generate_orders",
                max_retries=retries,
                environment_key="default",
                depends_on=[
                    TaskDependency(task_key="extract_source1"),
                    TaskDependency(task_key="extract_source2"),
                ],
                python_wheel_task=_wheel_task(),
            ),
            Task(
                task_key="generate_orders_agg",
                max_retries=retries,
                environment_key="default",
                depends_on=[TaskDependency(task_key="generate_orders")],
                python_wheel_task=_wheel_task(),
            ),
        ]
    )

    schedule = None
    email_notifications = None
    if environment == "prod":
        schedule = CronSchedule(quartz_cron_expression="0 0 5 * * ?", timezone_id="UTC")
        email_notifications = JobEmailNotifications(
            on_failure=_alert_emails(),
            on_duration_warning_threshold_exceeded=_alert_emails(),
        )

    job = Job(
        name=f"{JOB_NAME}_${{bundle.target}}",
        timeout_seconds=3600,
        tags=_tags(environment),
        environments=_environments(),
        schedule=schedule,
        email_notifications=email_notifications,
        tasks=tasks,
    )

    d = job.as_dict()
    d["deployment"] = {"kind": "BUNDLE"}

    if environment in ("staging", "prod"):
        # Pin the run-as identity to the SP application id rather than $current_user
        # so a developer running `make deploy env=staging` doesn't accidentally deploy
        # a job that runs as themselves.
        sp_id = os.environ.get("TEMPLATE_SP_APP_ID") or _get_service_principal_id(SP_DISPLAY_NAME, profile=environment)
        d["run_as"] = {"service_principal_name": sp_id}
        d["permissions"] = [{"service_principal_name": sp_id, "level": "CAN_MANAGE"}]

    return d


def _build_job_integration_test(environment: str) -> dict:
    tasks = [
        Task(
            task_key="setup",
            max_retries=0,
            environment_key="default",
            python_wheel_task=_wheel_task(),
        ),
        Task(
            task_key="run",
            depends_on=[TaskDependency(task_key="setup")],
            run_job_task=RunJobTask(job_id=f"${{{f'resources.jobs.{JOB_NAME}.id'}}}"),
        ),
        Task(
            task_key="validate",
            max_retries=0,
            environment_key="default",
            depends_on=[TaskDependency(task_key="run")],
            python_wheel_task=_wheel_task(),
        ),
    ]

    job = Job(
        name=f"{JOB_NAME}_${{bundle.target}}_integration_test",
        timeout_seconds=3600,
        tags=_tags(environment),
        environments=_environments(),
        tasks=tasks,
    )

    d = job.as_dict()
    d["deployment"] = {"kind": "BUNDLE"}

    if environment in ("staging", "prod"):
        sp_id = os.environ.get("TEMPLATE_SP_APP_ID") or _get_service_principal_id(SP_DISPLAY_NAME, profile=environment)
        d["run_as"] = {"service_principal_name": sp_id}
        d["permissions"] = [{"service_principal_name": sp_id, "level": "CAN_MANAGE"}]

    return d


def main():
    parser = argparse.ArgumentParser(description="Generate Databricks Jobs YAML")
    parser.add_argument("environment", help="Target environment (dev, staging, prod)")
    args = parser.parse_args()

    jobs: dict = {JOB_NAME: _build_job(args.environment)}
    if args.environment in ("dev", "staging"):
        jobs[f"{JOB_NAME}_integration_test"] = _build_job_integration_test(args.environment)

    output = {"resources": {"jobs": jobs}}

    output_file = "./resources/jobs.yml"
    with open(output_file, "w") as f:
        yaml.dump(output, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

    print(f"Generated {output_file}")


if __name__ == "__main__":
    main()
