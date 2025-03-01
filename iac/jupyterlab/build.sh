docker build -t dongoctuyen/jupyterlab:spark-base-v3.4 .
docker push dongoctuyen/jupyterlab:spark-base-v3.4
kubectl create namespace jupyterlab
kubectl create serviceaccount spark-driver -n jupyterlab
kubectl create clusterrolebinding spark-driver-rb --clusterrole=cluster-admin --serviceaccount=jupyterlab:spark-driver

