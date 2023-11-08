FROM apache/airflow:2.7.1

COPY --chown=airflow:root plugins /opt/airflow/plugins


FROM python:3
RUN pip install Cython \
  && pip install pyjnius
