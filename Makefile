.PHONY : develop check clean clean_pyc doc lint lint-diff black doc-references coverage tests

ELASTICSEARCH_URL="localhost:9200"

clean:
	-python setup.py clean

clean_pyc:
	-find ./tests -name "*.py[co]" -exec rm {} \;
	-find ./pandagg -name "*.py[co]" -exec rm {} \;

lint-diff:
	git diff upstream/master --name-only -- "*.py" | xargs flake8

lint:
	flake8 --count --show-source --statistics pandagg
	# on tests, more laxist: allow "missing whitespace after ','" and "line too long"
	flake8 --count --ignore=W503,W605,E231,E501 --show-source --statistics tests

black:
	black examples docs pandagg tests setup.py

develop:
	-python -m pip install -e ".[develop]"

doc-references:
	-make -C docs api-doc

es-up:
	docker compose up elasticsearch -d

es-down:
	docker compose down elasticsearch

tests:
	ELASTICSEARCH_URL=${ELASTICSEARCH_URL} pytest

mypy:
	mypy --install-types pandagg

coverage:
	coverage run --source=./pandagg -m pytest
	coverage report

check: doc-references black lint

create_dist: check
	python setup.py sdist bdist_wheel

test_dist: create_dist
	twine upload --skip-existing -r testpypi dist/*

upload_dist:
	twine upload --skip-existing dist/*
