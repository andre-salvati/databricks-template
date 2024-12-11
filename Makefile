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

deploy-staging:
	pipenv run python ./scripts/generate_template_workflow.py staging
	pipenv run databricks bundle deploy --target staging

run-staging:
	pipenv run databricks bundle run --target staging

deploy-prod:
	pipenv run python ./scripts/generate_template_workflow.py prod
	pipenv run databricks bundle deploy --target prod
