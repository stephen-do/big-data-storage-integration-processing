#!/bin/bash

kubectl apply -f resources/ns-airflow.yaml
kubectl apply -f resources/pvc-dags.yaml

helm repo add apache-airflow https://airflow.apache.org
helm repo update

helm install airflow apache-airflow/airflow -f helm/values.yaml -n airflow --debug \
  --set dags.persistence.enabled=true \
  --set dags.persistence.existingClaim=dags \
  --set dags.gitSync.enabled=false
