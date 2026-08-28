FROM apache/airflow:3.3.1-python3.13

COPY --chown=airflow:root orchestration/airflow-requirements.txt /tmp/airflow-requirements.txt

# Keep the installed Airflow package pinned to the base-image version. This also
# prevents pip from silently upgrading or downgrading Airflow while resolving
# the project runtime dependencies.
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    --requirement /tmp/airflow-requirements.txt

COPY --chown=airflow:root --chmod=0755 \
    infrastructure/airflow/start-standalone.sh \
    /usr/local/bin/industrial-energy-airflow-standalone

ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/industrial-energy/orchestration/dags \
    AIRFLOW__CORE__LOAD_EXAMPLES=False \
    PIPELINE_WORK_ROOT=/opt/airflow/work \
    PYTHONPATH=/opt/industrial-energy

WORKDIR /opt/industrial-energy

CMD ["bash", "-c", "/usr/local/bin/industrial-energy-airflow-standalone"]
