# Intro

This repo helps you deploy airflow to a local kubernetes cluster.

# Installation

1. Install `kubectl`
2. Run a local kubernetes cluster (e.g. Docker Desktop, minikube)
3. Install helm (e.g. `brew install helm`)
4. Point `kubectl` at your cluster (e.g. `kubectl config use-context docker-desktop`)
5. Run `./install.sh`

This will create an `airflow` namespace and deploy airflow to it.

You can connect to the webserver by:

1. Run `kubectl -n airflow port-forward svc/airflow-webserver 8080:8080` in a terminal
   - TODO: Fix stability with the port forward connection.
     Sometimes connections fail and you just have to keep retrying.
2. Navigate your browser to: <http://localhost:8080>
3. Login with user/pass: `admin`/`admin`

From the UI, you can create the DB connection:

1. Go to Connections in the Admin tab
2. Add a new connection:
   - Connection Id: `local_pg_conn`
   - Connection Type: `Postgres`
   - Host: `postgres`
   - Database: `airflow`
   - Login: `airflow`
   - Password: `airflow`
   - Port: `5432`

# Create a DAG

1. Create `your_dag.py`
2. Run `kubectl -n airflow get pod` to get the name of the scheduler pod
3. Run `kubectl cp your_dag.py airflow/<scheduler-pod-name>:/opt/airflow/dags/your_dag.py -c scheduler`
4. Unpause the DAG in the UI

# Uninstallation

Run `./uninstall.sh`. It can take a long time for the namespace to be deleted.

# Troubleshooting

If you're having issues when installing/setting it up check for your error message in this section to see if it can be resolved.

<details>
  <summary>error: no context exists with the name: "docker-desktop"</summary>
Make sure you have docker-desktop installed and running. Then on the docker-desktop dashboard go to: Settings > Kubernetes > Enable Kubernetes > Apply & Restart
</details>

# Refer
https://github.com/Wopple/local-kubernetes-airflow-quickstart
