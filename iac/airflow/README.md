## Deployment

run

```bash
helm upgrade --install airflow apache-airflow/airflow -f values.yaml -n airflow --create-namespace
```