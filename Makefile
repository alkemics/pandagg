.PHONY : develop check clean clean_pyc doc lint lint-diff black doc-references coverage

clean:
	-python setup.py clean

clean_pyc:
	-find ./tests -name "*.py[co]" -exec rm {} \;
	-find ./pandagg -name "*.py[co]" -exec rm {} \;

lint-diff:
	git diff upstream/master --name-only -- "*.py" | xargs flake8

lint:
	# ignore "line break before binary operator", and "invalid escape sequence '\_'" useful for doc
	flake8 --count --ignore=W503,W605 --show-source --statistics pandagg
	# on tests, more laxist: allow "missing whitespace after ','" and "line too long"
	flake8 --count --ignore=W503,W605,E231,E501 --show-source --statistics tests

black:
	black examples docs pandagg tests setup.py

develop:
	-python -m pip install -e ".[develop]"

doc-references:
	-make -C docs api-doc

tests:
	pytest

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
