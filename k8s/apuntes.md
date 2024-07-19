# K8s Kind Testing

[Kind Guide](https://airflow.apache.org/docs/helm-chart/stable/quick-start.html#add-airflow-helm-stable-repo)

The Astronomer stuff was weird - it was set to Airflow Version 2.0.0 for some reason. Docs were limited. Confusing how to upgrade using the Astronomer-provided versions.

Airflow provides an official Kind tutorial, but it uses a base Airflow image. Preferably I'd use the Astronomer Docker Images, but I couldn't get that to work with `k8s/Dockerfile`.

- But, seems pretty easy to use the standard Airflow Helm Chart way. And then just build your own Docker Images with your DAGs as needed.

``` sh
kind create cluster --name my-cluster

helm repo add apache-airflow https://airflow.apache.org
helm repo update

export NAMESPACE=example-namespace
kubectl create namespace $NAMESPACE

export RELEASE_NAME=example-release
helm install $RELEASE_NAME apache-airflow/airflow --namespace $NAMESPACE

kind load docker-image kind_astro_image:latest --name my-cluster

helm upgrade $RELEASE_NAME apache-airflow/airflow --namespace $NAMESPACE \
    --set images.airflow.repository=kind_astro_image \
    --set images.airflow.tag=latest

kubectl port-forward svc/$RELEASE_NAME-webserver 8080:8080 --namespace $NAMESPACE

kubectl create namespace airflow
helm repo add astronomer https://helm.astronomer.io
helm install airflow --namespace airflow astronomer/airflow

kubectl port-forward svc/airflow-webserver 8080:8080 --namespace airflow

kubectl delete pods --all --all-namespaces

kubectl delete namespace airflow
kubectl delete namespace example-namespace

kind delete cluster --name my-cluster

kubectl config current-context
kubectl get clusters
```
```