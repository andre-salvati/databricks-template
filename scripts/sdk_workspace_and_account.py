from databricks.sdk import WorkspaceClient
from databricks.sdk import AccountClient
import requests


def demoWorkspaceApi():
    workspace = WorkspaceClient(
        profile="dev"  # as configured in .databrickscfg
    )

    for j in workspace.jobs.list():
        print("Job: " + str(j.job_id))

    for c in workspace.catalogs.list():
        print("Catalog: " + c.name)
        schemas = workspace.schemas.list(catalog_name=c.name)
        for s in schemas:
            print("  Schema: " + s.name)


def demoAccountApi():
    # you need to run "databricks auth login -p <accountProfile>" first...
    a = AccountClient(profile="account1")

    print(a)

    for w in a.workspaces.list():
        print("Workspace: " + w.workspace_name)


if __name__ == "__main__":
    demoWorkspaceApi()

    demoAccountApi()
