package:
	rm -Rf dist
	python setup.py sdist bdist_wheel

install: package
	pip uninstall kedro-rich -y
	pip install -U dist/*.whl

install-pip-setuptools:
	python -m pip install -U pip setuptools wheel

lint:
	pre-commit run -a --hook-stage manual

test:
	pytest -vv tests

secret-scan:
	trufflehog --max_depth 1 --exclude_paths trufflehog-ignore.txt .

clean:
	rm -rf build dist pip-wheel-metadata .pytest_cache
	find . -regex ".*/__pycache__" -exec rm -rf {} +
	find . -regex ".*\.egg-info" -exec rm -rf {} +
	rm -rf test_project/

install-test-requirements:
	pip install -r test_requirements.txt

install-pre-commit: install-test-requirements
	pre-commit install --install-hooks

uninstall-pre-commit:
	pre-commit uninstall
	pre-commit uninstall --hook-type pre-push

test-proj:
	pip install . -e
	make install
	rm -rf test_project/
	yes test_project | kedro new --starter=spaceflights
	pip install -r test_project/src/requirements.txt
	touch .telemetry
	echo "consent: false" >> .telemetry
	mv .telemetry test_project/

test-run:
	cd test_project; kedro rrun

clear-test-run:
	rm -rf test_project/
