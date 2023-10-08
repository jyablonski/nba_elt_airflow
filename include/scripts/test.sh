#!/bin/bash

clear
echo -n -e "RDS Endpoint: "
read EP
echo -n -e "Port: "
read PORT
echo -n -e "Database Name: "
read DBNAME

echo $EP
echo $PORT
echo $DBNAME
echo $TEST_ENV_VAR
echo $AIRFLOW_ENV_VAR

aws sts get-caller-identity

echo exiting out ...