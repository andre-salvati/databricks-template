import argparse

import yaml
from databricks.sdk import WorkspaceClient
from databricks.bundles.jobs import (
    CronSchedule,
    Job,
    JobEnvironment,
    RunJobTask,
    Task,
    TaskDependency,
    PythonWheelTask,
)
from databricks.bundles.jobs._models.environment import Environment


SP_DISPLAY_NAME = "template-sp"


def _get_service_principal_id(display_name: str) -> int:
    workspace = WorkspaceClient(profile="dev")
    for sp in workspace.service_principals.list():
        if sp.display_name == display_name:
            return sp.application_id
    raise ValueError(f"Service principal '{display_name}' not found in workspace.")


def _wheel_task(schema: str) -> PythonWheelTask:
    return PythonWheelTask(
        package_name="template",
        entry_point="main",
        parameters=[
            "--task={{task.name}}",
            "--env=${bundle.target}",
            "--user=${workspace.current_user.short_name}",
            f"--schema={schema}",
            "${var.debug}",
        ],
    )


def _environments() -> list[JobEnvironment]:
    return [
        JobEnvironment(
            environment_key="default",
            spec=Environment(client="5", dependencies=["../dist/template-*.whl"]),
        )
    ]


def _tags() -> dict[str, str]:
    return {
        "git_branch": "${bundle.git.branch}",
        "git_origin_url": "${bundle.git.origin_url}",
    }


def _build_job1(environment: str) -> dict:
    tasks = [
        Task(
            task_key="extract_source1",
            max_retries=0,
            environment_key="default",
            python_wheel_task=_wheel_task("raw"),
        ),
        Task(
            task_key="extract_source2",
            max_retries=0,
            environment_key="default",
            python_wheel_task=_wheel_task("raw"),
        ),
        Task(
            task_key="generate_orders",
            max_retries=0,
            environment_key="default",
            depends_on=[
                TaskDependency(task_key="extract_source1"),
                TaskDependency(task_key="extract_source2"),
            ],
            python_wheel_task=_wheel_task("curated"),
        ),
        Task(
            task_key="generate_orders_agg",
            max_retries=0,
            environment_key="default",
            depends_on=[TaskDependency(task_key="generate_orders")],
            python_wheel_task=_wheel_task("report"),
        ),
    ]

    schedule = None
    if environment == "prod":
        schedule = CronSchedule(quartz_cron_expression="0 0 5 * * ?", timezone_id="UTC")

    job = Job(
        name="template_${bundle.target}",
        timeout_seconds=3600,
        tags=_tags(),
        environments=_environments(),
        schedule=schedule,
        tasks=tasks,
    )

    d = job.as_dict()
    d["deployment"] = {"kind": "BUNDLE"}
    if environment in ("staging", "prod"):
        sp_id = _get_service_principal_id(SP_DISPLAY_NAME)
        d["run_as"] = {"service_principal_name": sp_id}
    return d


def _build_integration_test_job() -> dict:
    tasks = [
        Task(
            task_key="setup",
            max_retries=0,
            environment_key="default",
            python_wheel_task=_wheel_task("external_source"),
        ),
        Task(
            task_key="run",
            depends_on=[TaskDependency(task_key="setup")],
            run_job_task=RunJobTask(job_id="${resources.jobs.job1.id}"),
        ),
        Task(
            task_key="validate",
            max_retries=0,
            environment_key="default",
            depends_on=[TaskDependency(task_key="run")],
            python_wheel_task=_wheel_task("report"),
        ),
    ]

    job = Job(
        name="template_${bundle.target}_integration_test",
        timeout_seconds=3600,
        tags=_tags(),
        environments=_environments(),
        tasks=tasks,
    )

    return job.as_dict()


def main():
    parser = argparse.ArgumentParser(description="Generate Databricks workflow YAML")
    parser.add_argument("environment", help="Target environment (dev, staging, prod)")
    parser.add_argument(
        "--service-principal-id",
        default=None,
        help="Application ID of the service principal to set as the job run-as identity",
    )
    args = parser.parse_args()

    jobs: dict = {"job1": _build_job1(args.environment)}
    if args.environment in ("dev", "staging"):
        jobs["integration_test_job"] = _build_integration_test_job()

    output = {"resources": {"jobs": jobs}}

    output_file = "./resources/jobs.yml"
    with open(output_file, "w") as f:
        yaml.dump(output, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

    print(f"Generated {output_file}")


if __name__ == "__main__":
    main()
