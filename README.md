
## Project Template for a CI/CD Pipeline with a PySpark/Databricks Project

This project template provides a structured approach to enhance your productivity when delivering data pipelines on Databricks. Feel free to further customize it based on your project's specific nuances and the audience you are targeting.

This project template demonstrates how to:

- structure your code inside Python classes / packages.
- Configure your project to optimize dev and prod environments.
- Set up VS Code to execute local unit tests for your transformations.
- Execute a CI/CD pipeline with [Github Actions](https://docs.github.com/en/actions) after a push.
- Utilize [Databricks Workflows](https://docs.databricks.com/en/workflows/index.html) to execute a DAG (refer to the diagram below).
- Utilize [Databricks CLI and DBX](https://docs.databricks.com/en/dev-tools/cli/index.html) and DBX to package/deploy/run a Python package on Databricks.
- Utilize [Databricks Workflow parameters](https://community.databricks.com/t5/data-engineering/passing-a-date-parameter-through-workflow/td-p/9845) to manage DAG execution.
- Utilize [funcy package](https://pypi.org/project/funcy/) to log the execution time of each transformation.
- Utilize [chispa package](https://pypi.org/project/chispa/) to validate the outputted dataframes from your transformations.


<br>

<img src="docs/dag.png"  width="70%" height="70%">

<br>

## Prepare local env  

- build python env and execute unit tests

        pipenv --python 3.10
        pipenv shell
        pip install -r unit-requirements.txt
        pytest tests/

- configure databricks tools, deploy and execute on "dev" aws account

        databricks configure -t *token* (generate token on your Databricks workspace)
        databricks workspace ls /
        dbx deploy wf_template_dev --environment=dev
        dbx launch wf_template_dev --environment=dev
        
- configure VSCode

        Python: Create Environment (??)
        Python: Configure Tests (??)


## CI/CD pipeline  

This diagram illustrates the CI/CD pipeline for this project.

<br>

<img src="docs/ci_cd.png"  width="70%" height="70%">

<br>

## Possible improvements

- Introduce Delta Live Tables
- Introduce Databricks Connect
- Introduce Databricks Asset Bundle (preview)

