.PHONY : develop check clean clean_pyc doc lint-diff black doc-references

clean:
	-python setup.py clean

clean_pyc:
	-find ./tests -name "*.py[co]" -exec rm {} \;
	-find ./pandagg -name "*.py[co]" -exec rm {} \;

lint-diff:
	git diff upstream/master --name-only -- "*.py" | xargs flake8

black:
	black examples docs pandagg tests setup.py

develop:
	-python -m pip install -e .

doc-references:
	-make -C docs api-doc

check: black doc-references
