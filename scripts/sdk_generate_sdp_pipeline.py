"""
Generate resources/jobs_sdp.yml for the job1_sdp Spark Declarative Pipeline.

Mirrors the pattern of sdk_generate_template_job.py:
  - Called by `make deploy env=<env>` with the target environment as the sole argument.
  - Produces a single YAML file that the bundle picks up via `include: resources/*.yml`.
  - For staging/prod: injects run_as, permissions, and target-level workspace overrides
    pinned to the service principal so `mode: production`'s deployer-identity check
    has something concrete to enforce against.

Do NOT hand-edit resources/jobs_sdp.yml — it is overwritten on every deploy.
"""

import argparse
import os
import re
import tomllib
from pathlib import Path

import yaml
from databricks.bundles.pipelines import (
    FileLibrary,
    Pipeline,
    PipelineLibrary,
    PipelinePermission,
    PipelinePermissionLevel,
    PipelinesEnvironment,
)
from databricks.bundles.pipelines._models.run_as import RunAs
from databricks.sdk import WorkspaceClient

PIPELINE_NAME = "job1_sdp"

# Service principal name. "make init" overrides with parameter.
SP_DISPLAY_NAME = "template-sp"

# Used as the team / cost-center tag. Surfaces in system.billing.usage.
COST_CENTER = "data-platform"
TEAM = "data-engineering"


def _project_version() -> str:
    """Read version from pyproject.toml so the bundle pins to the exact built wheel."""
    pyproject = Path(__file__).resolve().parent.parent / "pyproject.toml"
    with open(pyproject, "rb") as f:
        return tomllib.load(f)["project"]["version"]


# Path is relative to the bundle root (databricks.yml location).
WHEEL_GLOB = f"../dist/template-{_project_version()}-py3-none-any.whl"

# Pin DQX to the same version declared in pyproject.toml dependencies.
DQX_PACKAGE = "databricks-labs-dqx==0.12.0"


def _get_service_principal_id(display_name: str, profile: str) -> int:
    workspace = WorkspaceClient(profile=profile)
    for sp in workspace.service_principals.list():
        if sp.display_name == display_name:
            return sp.application_id
    raise ValueError(f"Service principal '{display_name}' not found in workspace.")


def _resolve_catalog(environment: str) -> str:
    """
    Compute the target catalog name at generation time, mirroring Config.__init__.

    Resolving the catalog here (rather than at pipeline runtime) lets us set
    the mandatory `catalog` field that serverless pipelines require, and avoids
    a WorkspaceClient call inside the SDP pipeline itself.

    - dev  → dev_<sanitized_user>  (developer sandbox; same sanitization as Config)
    - staging / prod → environment name
    """
    if environment == "dev":
        ws = WorkspaceClient(profile="dev")
        local_part = ws.current_user.me().user_name.split("@")[0]
        user = re.sub(r"[^a-zA-Z0-9_]", "_", local_part)
        return f"dev_{user}"
    return environment


def _tags(environment: str) -> dict[str, str]:
    return {
        "git_branch": "${bundle.git.branch}",
        "git_origin_url": "${bundle.git.origin_url}",
        "environment": environment,
        "cost_center": COST_CENTER,
        "team": TEAM,
    }


def _build_pipeline(environment: str, catalog: str, sp_id: str | None) -> dict:
    """
    Build the pipeline resource definition for the given environment.

    Configuration:
      - `catalog` is resolved at generation time (WorkspaceClient for dev,
        environment name for staging/prod) and stamped into both the mandatory
        `catalog` field (required by serverless) and the `target_catalog` conf
        parameter that pipeline.py reads via spark.conf.get("target_catalog").
      - `development=True` on dev enables smaller clusters and faster iteration.
      - On staging/prod: run_as and permissions are pinned to the SP so the
        pipeline runs under the SP identity regardless of who triggers it.
    """
    pipeline = Pipeline(
        name=f"{PIPELINE_NAME}_${{bundle.target}}",
        serverless=True,
        catalog=catalog,
        # UC serverless pipelines require a target schema. All tables in this
        # pipeline use fully qualified names, so this is just the default for
        # any unqualified references and for the pipeline's event log location.
        schema="raw",
        development=(environment == "dev"),
        libraries=[
            # Path is relative to resources/jobs_sdp.yml, so ../ climbs to
            # the bundle root — same convention as WHEEL_GLOB above.
            PipelineLibrary(file=FileLibrary(path="../src/template/job1_sdp/pipeline.py"))
        ],
        # PipelinesEnvironment.dependencies installs Python packages into the
        # serverless pipeline environment. The project wheel is needed so
        # pipeline.py can import template.job1_sdp.transforms.
        environment=PipelinesEnvironment(dependencies=[WHEEL_GLOB, DQX_PACKAGE]),
        # target_catalog is read by pipeline.py via spark.conf.get("target_catalog")
        # to build fully qualified table names. Passing it explicitly avoids a
        # WorkspaceClient call inside the SDP pipeline at runtime.
        configuration={"target_catalog": catalog},
        tags=_tags(environment),
    )

    d = pipeline.as_dict()

    if environment in ("staging", "prod"):
        # Explicit pipeline-level run_as so the pipeline always runs as the SP,
        # even if triggered manually by a developer. Mirrors the job-level
        # run_as in sdk_generate_template_job.py.
        d["run_as"] = {"service_principal_name": sp_id}
        # Grant IS_OWNER to the SP so it can update/restart the pipeline.
        d["permissions"] = [{"service_principal_name": sp_id, "level": "IS_OWNER"}]

    return d


def _target_overrides(environment: str, sp_id: str) -> dict:
    """
    Target-level run_as and root_path pinned to the SP for staging/prod.

    Mirrors sdk_generate_template_job.py._target_overrides so both generated
    files contribute the same values to the merged target block — see that
    module's docstring for the two-gate reasoning.
    """
    return {
        "run_as": {"service_principal_name": sp_id},
        "workspace": {"root_path": f"/Workspace/Users/{sp_id}/.bundle/{environment}/${{bundle.name}}"},
    }


def main():
    parser = argparse.ArgumentParser(description="Generate Databricks SDP Pipeline YAML")
    parser.add_argument("environment", help="Target environment (dev, staging, prod)")
    args = parser.parse_args()

    env = args.environment

    # Resolve SP app id once for staging/prod; reused for pipeline-level
    # run_as/permissions and target-level overrides.
    sp_id: str | None = None
    if env in ("staging", "prod"):
        sp_id = os.environ.get("TEMPLATE_SP_APP_ID") or _get_service_principal_id(SP_DISPLAY_NAME, profile=env)

    catalog = _resolve_catalog(env)

    output: dict = {"resources": {"pipelines": {PIPELINE_NAME: _build_pipeline(env, catalog, sp_id)}}}

    if sp_id is not None:
        output["targets"] = {env: _target_overrides(env, sp_id)}

    output_file = "./resources/jobs_sdp.yml"
    with open(output_file, "w") as f:
        yaml.dump(output, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

    print(f"Generated {output_file}")


if __name__ == "__main__":
    main()
