```argo
kubectl kustomize --enable-helm bootstrap/argocd | kubectl apply -f -
kubectl port-forward -n argocd svc/argocd-server 8080:443
kubectl get secret -n argocd argocd-initial-admin-secret -ojsonpath={.data.password} | base64 --decode

```