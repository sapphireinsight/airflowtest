## Build 

Build new airflow docker image
```
docker built . -t zhaoqi0406/airflow:2.7.1.x
docker push zhaoqi0406/airflow:2.7.1.x
```
Update docker image version in airflow values file


## Deployment

```
helm --kube-context ethos103-stage-or2 --namespace ns-team-mkto-sapphire-insight-batch-poc-or2 upgrade --install airflow apache-airflow/airflow -f airflow-values.yaml
```