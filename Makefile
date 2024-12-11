install:
	python3 -m pip install --upgrade pip
	pip install pipenv
	pipenv install packages
	pipenv run pytest tests/
	pipenv run pip list
	pipenv shell

install-ci:
	python3 -m pip install --upgrade pip
	pip install pipenv
	pip install packages
	pytest tests/
	pip list

pre-commit:
	pre-commit autoupdate
	pre-commit run --all-files

deploy-dev:
	python ./scripts/generate_template_workflow.py dev
	databricks bundle deploy --target dev

run-dev:
	databricks bundle run default_python_job --target dev

deploy-ci:
	pipenv run python ./scripts/generate_template_workflow.py prod
	pipenv run databricks bundle deploy --target prod
