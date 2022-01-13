# Lints all python files
.PHONY: deploy-docker
deploy-docker: 
	docker build -t nba_airflow .
	aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 324816727452.dkr.ecr.us-east-1.amazonaws.com
	docker tag nba_airflow:latest 324816727452.dkr.ecr.us-east-1.amazonaws.com/jacobs_repo:nba_airflow
	docker push 324816727452.dkr.ecr.us-east-1.amazonaws.com/jacobs_repo:nba_airflow

.PHONY: test-file
test-file:
	@echo hi