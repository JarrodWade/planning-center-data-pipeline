####################################################################################################################
# Setup containers to run Airflow

docker-spin-up:
	docker compose --env-file env up airflow-init && docker compose --env-file env up --build -d

perms:
	mkdir -p logs plugins temp dags tests && chmod -R u=rwx,g=rwx,o=rwx logs plugins temp dags tests 

up: perms docker-spin-up

down:
	docker compose down

sh:
	docker exec -ti webserver bash

####################################################################################################################
# Testing, auto formatting, type checks, & Lint checks

pytest:
	docker exec webserver pytest -p no:warnings -v /opt/airflow/tests

format:
	docker exec webserver python -m black -S --line-length 79 .

isort:
	docker exec webserver isort .

type:
	docker exec webserver mypy --ignore-missing-imports /opt/airflow

lint: 
	docker exec webserver flake8 /opt/airflow/dags

ci: isort format type lint pytest