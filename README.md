# Airflow Project Version: 1.1.6
This Repo is a local Airflow Deployment for my [NBA Project](https://github.com/jyablonski/NBA-Dashboard), with functional `ECSOperator` and `dbt run` tasks for the project workflow.  This is not currently used for the production ELT, as I cannot host Airflow on any Cloud provider for free.  

![NBA ELT Pipeline Data Flow 2 (1)](https://user-images.githubusercontent.com/16946556/161156854-9ba5583e-94ed-4c80-9699-c2c50f41ed1c.jpg)

The template I used to get started is available [here](https://github.com/soggycactus/airflow-repo-template).

* Links to other Repos providing infrastructure for this Project

	* [Python Web Scrape](https://github.com/jyablonski/python_docker)
	* [ML Pipeline](https://github.com/jyablonski/nba_elt_mlflow)
	* [Terraform](https://github.com/jyablonski/aws_terraform/)
	* [dbt](https://github.com/jyablonski/nba_elt_dbt)
	* [Shiny Server](https://github.com/jyablonski/NBA-Dashboard)