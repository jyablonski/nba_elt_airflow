# old packages
airflow-provider-great-expectations==0.0.8
airflow-dbt==0.4.0

Removing these packages as of 2022-05-01 bc of dependency issues.

# helper script
script below can create theconnections without having to manually do it ??
from airflow.models.connection import Connection

c = Connection(conn_id='my_connection',
               conn_type='postgresql',
               host='localhost',
               login='postgres',
               password='postgres',
               port=5432,
               schema='mydb',
               extra={"param1": "val1"})

uri = c.get_uri()

print(uri)

# docker compose postgres example
[link](https://github.com/apache/airflow/blob/05b44099459a7e698c3df88cec1bcad145748448/scripts/ci/docker-compose/backend-postgres.yml#L23)


#### EC2 Notes
```
sudo yum update -y
sudo yum upgrade -y
sudo yum install git -y
git init

sudo amazon-linux-extras install docker
sudo service docker start
sudo usermod -a -G docker ec2-user


sudo amazon-linux-extras enable python3.8
sudo yum install python3.8

sudo yum -y update
sudo yum -y groupinstall "Development Tools"
sudo yum -y install openssl-devel bzip2-devel libffi-devel

sudo curl -L https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m) -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
docker-compose version

-- REBOOT AT THIS POINT (ec2 -> reboot instance)

git pull https://github.com/jyablonski/nba_elt_airflow.git

make start-airflow

```

# LOGIN STUFF
`http://ec2-3-85-30-184.compute-1.amazonaws.com:8080/` user: tootis123 password: admin\
