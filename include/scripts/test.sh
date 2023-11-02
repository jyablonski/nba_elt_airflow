#!/bin/bash

# clear
# echo -n -e "RDS Endpoint: "
# read EP
# echo -n -e "Port: "
# read PORT
# echo -n -e "Database Name: "
# read DBNAME

echo $EP
echo $RDSPORT
echo $DBNAME
echo $COMNAME
echo $AIRFLOW_ENV_VAR
echo $MASTERUSER
echo $MYPASS

aws sts get-caller-identity

echo exiting out ...