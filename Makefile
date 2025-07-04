env ?= dev

install:
	python3 -m pip install --upgrade pip
	pip install pipenv
	pipenv install packages
	pipenv run pytest tests/ --cov=src --cov-report=xml:coverage_reports/coverage.xml --cov-report=html:coverage_reports/html
	pipenv run pip list

pre-commit:
	pre-commit autoupdate
	pre-commit run --all-files

deploy:
	pipenv run python ./scripts/generate_template_workflow.py $(env)
	pipenv run databricks bundle deploy --target $(env)

deploy-serverless:
	pipenv run python ./scripts/generate_template_workflow.py $(env) --serverless
	pipenv run databricks bundle deploy --target $(env)

run:
	pipenv run databricks bundle run default_python_job --target $(env)

