#!/bin/bash

helm uninstall airflow -n airflow
kubectl -n airflow delete pvc/dags
kubectl delete ns/airflow
