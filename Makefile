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
	databricks bundle deploy --target dev

deploy-ci:
	pipenv run python ./scripts/generate_template_workflow.py ci
	pipenv run databricks bundle deploy --target ci

run-ci:
	pipenv run databricks bundle run --target ci

deploy-prod:
	pipenv run python ./scripts/generate_template_workflow.py prod
	pipenv run databricks bundle deploy --target prod
