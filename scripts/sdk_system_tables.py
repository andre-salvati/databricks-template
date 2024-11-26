from databricks.sdk import WorkspaceClient
from databricks.sdk import AccountClient
import requests

host = None
token = None


def demoWorkspaceApi(w):
    for j in w.jobs.list():
        print("Job: " + str(j.job_id))

    for c in w.catalogs.list():
        print("Catalog: " + c.name)
        schemas = w.schemas.list(catalog_name=c.name)
        for s in schemas:
            print("  Schema: " + s.name)


def demoAccountApi():
    a = AccountClient(profile="account")

    print(a)

    for m in a.metastores.list():
        print("Metastore: " + m.metastore_id)
        metastore = m

    return metastore


def enable(system_tables, metastore):
    print("Enabling " + system_tables + " tables for " + metastore.name + " ...")

    update = f"{host}/api/2.0/unity-catalog/metastores/{metastore.metastore_id}/systemschemas/{system_tables}"
    response = requests.put(update, headers=token)

    if response.status_code == 200:
        print("OK")
    else:
        print("Failed")
        print(response.text)


def enableSystemTables(metastore):
    enable("billing", metastore)

    enable("access", metastore)

    enable("storage", metastore)

    enable("compute", metastore)

    enable("marketplace", metastore)

    enable("lineage", metastore)

    list = f"{host}/api/2.0/unity-catalog/metastores/a238eb20-95d3-4a62-91ea-629992514227/systemschemas"
    response = requests.get(list, headers=token)
    print(response.text)


if __name__ == "__main__":
    workspace = WorkspaceClient(
        profile="dev"  # as configured in .databrickscfg
    )

    print(workspace)

    token = workspace.config.authenticate()
    host = workspace.config.host

    demoWorkspaceApi(workspace)

    metastore = demoAccountApi()

    enableSystemTables(metastore)
