install:
	python3 -m pip install --upgrade pip
	pip install pipenv
	pipenv install packages
	pipenv run pytest tests/
	pipenv run pip list

pre-commit:
	pre-commit autoupdate
	pre-commit run --all-files

deploy-dev:
	python ./scripts/generate_template_workflow.py dev
	databricks bundle run default_python_job --target dev

deploy-run-ci:
	pipenv run python ./scripts/generate_template_workflow.py stag
	pipenv run databricks bundle deploy --target stag

deploy-prod:
	pipenv run databricks bundle deploy --target prod
