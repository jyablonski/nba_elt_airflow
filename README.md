# Airflow Project Version: 2.1.0
![Tests](https://github.com/jyablonski/nba_elt_airflow/actions/workflows/tests.yml/badge.svg) ![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)

This Repo is a local Airflow Deployment for my [NBA Project](https://github.com/jyablonski/NBA-Dashboard), with functional `EcsRunTaskOperator` tasks for the Web Scrape, dbt, and ML Jobs in the NBA ELT Pipeline. 

* Discord and Slack webhooks are both available and will execute in the event of a Task Fail during a DAG run.
* This is not currently used for the production Pipeline, as I cannot host Airflow on any Cloud provider for free.  

![airflow_workflow](https://user-images.githubusercontent.com/16946556/176963452-8621f4c1-cf8e-4124-a1f2-ab7d28b99069.jpg)

* Links to other Repos providing infrastructure for this Project

	* [Python Web Scrape](https://github.com/jyablonski/python_docker)
	* [ML Pipeline](https://github.com/jyablonski/nba_elt_mlflow)
	* [Terraform](https://github.com/jyablonski/aws_terraform/)
	* [dbt](https://github.com/jyablonski/nba_elt_dbt)
	* [Shiny Server](https://github.com/jyablonski/NBA-Dashboard)
	* [GraphQL API](https://github.com/jyablonski/graphql_praq)
	* [REST API](https://github.com/jyablonski/nba_elt_rest_api)
  