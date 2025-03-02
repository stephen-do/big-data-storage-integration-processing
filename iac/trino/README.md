## Deployment

run

```bash
helm repo add trino https://trinodb.github.io/charts
helm upgrade --install trino-cluster trino/trino -f values.yaml -n trino --create-namespace
```