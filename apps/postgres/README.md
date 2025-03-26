```postgres
1/ helm install my-postgresql bitnami/postgresql -f values.yaml --namespace postgres --create-namespace
2/ Inside the webserver pod, run the command to initialize airflow database
airflow db init
```