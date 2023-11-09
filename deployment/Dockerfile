FROM apache/airflow:2.7.1

USER root

RUN apt-get update \
  && apt-get install -y default-jdk \
  && apt-get clean

COPY --chown=airflow:root plugins /opt/airflow/plugins

COPY --chown=airflow:root jars /opt/airflow/jars

USER airflow

# Install OpenJDK-8
#FROM ubuntu:18.04
#USER root
#RUN apt-get update && \
#    apt-get install -y openjdk-8-jdk && \
#    apt-get install -y ant && \
#    apt-get clean;
    
# # Fix certificate issues
# RUN apt-get update && \
#     apt-get install ca-certificates-java && \
#     apt-get clean && \
#     update-ca-certificates -f;

# # Setup JAVA_HOME -- useful for docker commandline
# ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
# RUN export JAVA_HOME
