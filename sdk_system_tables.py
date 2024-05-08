from databricks.sdk import WorkspaceClient
from databricks.sdk import AccountClient
import requests

w = WorkspaceClient(
  profile = "dev"    # as configured in .databrickscfg
)

print(w)

token = w.config.authenticate()
host = w.config.host

def enable(system_tables, metastore):

  print("Enabling " + system_tables + " tables for " + metastore.name + " ...")

  update = f"{host}/api/2.0/unity-catalog/metastores/{metastore.metastore_id}/systemschemas/{system_tables}"
  response = requests.put(update, headers=token)

  if response.status_code == 200:
    print("OK")
  else:
    print("Failed")
    print(response.text)
 

# Demo for Workspace API

for j in w.jobs.list():
  print("Job: " + str(j.job_id))

for c in w.catalogs.list():
  print("Catalog: " + c.name)
  schemas = w.schemas.list(catalog_name=c.name)
  for s in schemas:
    print("  Schema: " + s.name)

# Demo for Account API

a = AccountClient(profile="account")

print(a)

for m in a.metastores.list():
  print("Metastore: " + m.metastore_id)
  metastore = m

# Enabling system tables

enable("billing", metastore)

enable("access", metastore)

enable("storage", metastore)

enable("compute", metastore)

enable("marketplace", metastore)

enable("lineage", metastore)

list = f"{host}/api/2.0/unity-catalog/metastores/a238eb20-95d3-4a62-91ea-629992514227/systemschemas"
response = requests.get(list, headers=token)
print(response.text)

