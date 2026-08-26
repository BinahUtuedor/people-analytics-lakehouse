FROM eclipse-temurin:17.0.16_8-jdk-jammy AS java

FROM python:3.11.13-slim-bookworm

COPY --from=java /opt/java/openjdk /opt/java/openjdk

ENV JAVA_HOME=/opt/java/openjdk \
    PATH="/opt/java/openjdk/bin:${PATH}" \
    PYSPARK_PYTHON=/usr/local/bin/python \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SPARK_LOCAL_IP=127.0.0.1

WORKDIR /workspace

COPY requirements-emr.txt requirements-emr-compat-tests.txt /tmp/

RUN python -m pip install --no-cache-dir \
        --requirement /tmp/requirements-emr-compat-tests.txt \
    && python --version \
    && java -version \
    && python -c "import pyspark; print(f'PySpark {pyspark.__version__}')"

CMD ["python", "-m", "unittest", "discover", "-s", "tests/spark/bronze", "-p", "test_*.py", "-v"]
