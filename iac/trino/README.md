## Deployment

run

```bash
helm repo add trino https://trinodb.github.io/charts
helm upgrade --install trino-cluster trino/trino -f values.yaml -n trino --create-namespace
```

<!-- The provided commands are used to deploy a Trino cluster using Helm, a package manager for Kubernetes. Here's a breakdown of the structure of these commands:

### Command 1: Add the Trino Helm Repository
```bash
helm repo add trino https://trinodb.github.io/charts
```
- `helm repo add`: This command adds a new Helm chart repository.
- `trino`: The name you are assigning to the repository.
- `https://trinodb.github.io/charts`: The URL of the Trino Helm chart repository.

This command registers the Trino Helm chart repository with your local Helm client, allowing you to install charts from this repository.

### Command 2: Install or Upgrade the Trino Cluster
```bash
helm upgrade --install trino-cluster trino/trino -f values.yaml -n trino --create-namespace
```
- `helm upgrade --install`: This command upgrades an existing release or installs a new release if it doesn't already exist.
- `trino-cluster`: The name of the Helm release. This is the name you will use to refer to this deployment.
- `trino/trino`: The chart to install, specified as `<repository>/<chart>`. Here, it refers to the `trino` chart from the `trino` repository added earlier.
- `-f values.yaml`: Specifies a custom values file (`values.yaml`) to use for the deployment. This file contains configuration values that override the default values provided by the chart.
- `-n trino`: Specifies the namespace (`trino`) in which to install the release. If the namespace does not exist, it will be created.
- `--create-namespace`: Ensures that the specified namespace (`trino`) is created if it does not already exist.

### Summary

These commands first add the Trino Helm chart repository to your local Helm client. Then, they install or upgrade a Trino cluster using the specified chart and configuration values, deploying it into the `trino` namespace. If the namespace does not exist, it will be created automatically. -->