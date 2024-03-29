# Use the official Apache Airflow base image with Ubuntu
FROM apache/airflow:2.8.3

# Set the environment variables
ENV AIRFLOW_HOME=/opt/airflow

# Switch to root user to perform system-level installations
USER root

# Copy and install Python dependencies
COPY requirements.txt .

USER airflow
RUN pip install --no-cache-dir -r requirements.txt

# Set the working directory
WORKDIR $AIRFLOW_HOME

# Copy DAGs and scripts directories from the host to the Docker image
COPY --chown=airflow:airflow dags /opt/airflow/dags
COPY --chown=airflow:airflow config/airflow.cfg /opt/airflow/airflow.cfg
COPY --chown=airflow:airflow config/webserver_config.py /opt/airflow/webserver_config.py
COPY --chown=airflow:airflow main_dataset/cleaned_datasets/ /opt/airflow/main_dataset/cleaned_datasets/
COPY --chown=airflow:airflow extra_dataset /opt/airflow/extra_dataset
COPY --chown=airflow:airflow clean_py /opt/airflow/clean_py

# Set correct permissions for the copied files
USER root
RUN chmod -R 755 /opt/airflow/dags 
RUN chmod -R 755 /opt/airflow/airflow.cfg 
RUN chmod -R 755 /opt/airflow/webserver_config.py 
RUN chmod -R 775 /opt/airflow/main_dataset/cleaned_datasets/
RUN chmod -R 755 /opt/airflow/extra_dataset
RUN chmod -R 755 /opt/airflow/clean_py
#/opt/airflow/main_dataset
USER airflow
