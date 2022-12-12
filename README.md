# Airflow Project Version: 2.0.2
This Repo is a local Airflow Deployment for my [NBA Project](https://github.com/jyablonski/NBA-Dashboard), with functional `EcsRunTaskOperator` tasks for the Web Scrape, dbt, and ML Jobs in my ELT Pipeline. 

* Discord and Slack webhooks are both available and will execute in the event of a Task Fail during a DAG run.
* This is not currently used for the production Pipeline, as I cannot host Airflow on any Cloud provider for free.  

![airflow_workflow](https://user-images.githubusercontent.com/16946556/176963452-8621f4c1-cf8e-4124-a1f2-ab7d28b99069.jpg)

The template I used to get started is available [here](https://github.com/soggycactus/airflow-repo-template).

* Links to other Repos providing infrastructure for this Project

	* [Python Web Scrape](https://github.com/jyablonski/python_docker)
	* [ML Pipeline](https://github.com/jyablonski/nba_elt_mlflow)
	* [Terraform](https://github.com/jyablonski/aws_terraform/)
	* [dbt](https://github.com/jyablonski/nba_elt_dbt)
	* [Shiny Server](https://github.com/jyablonski/NBA-Dashboard)
	* [GraphQL API](https://github.com/jyablonski/graphql_praq)
  