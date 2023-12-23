
This project template demonstrates:
	
- how to configure VS Code to run local unit tests for transformations  	
- how to use Databricks Workflows to run a DAG (see diagram)
- how to use Databricks CLI and DBX to pack / deploy / run a Python package on Databricks
- how to configure your project to use dev and prod environments
- how to run a CI/CD pipeline with Github Actions after push
- the use of parameters to control DAG configurations	
- the use of funcy to log the execution time of each transformation.

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

This is .....

<br>

<img src="docs/ci_cd.png"  width="70%" height="70%">

<br>

## Possible improvements

- Use Delta Live Tables

