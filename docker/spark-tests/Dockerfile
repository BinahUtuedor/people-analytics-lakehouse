FROM eclipse-temurin:21.0.8_9-jdk-jammy@sha256:adb9b2d15adf1833d9dae0bdc1cff61ef5a804dc58dfbfb34269f32432b2e5dc AS java

FROM python:3.12.10-slim-bookworm@sha256:fd95fa221297a88e1cf49c55ec1828edd7c5a428187e67b5d1805692d11588db

COPY --from=java /opt/java/openjdk /opt/java/openjdk

ENV JAVA_HOME=/opt/java/openjdk \
    PATH="/opt/java/openjdk/bin:${PATH}" \
    PYSPARK_PYTHON=/usr/local/bin/python \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SPARK_LOCAL_IP=127.0.0.1

WORKDIR /workspace

COPY requirements-spark-tests.txt /tmp/requirements-spark-tests.txt

RUN python -m pip install --no-cache-dir \
        --requirement /tmp/requirements-spark-tests.txt \
    && python --version \
    && java -version \
    && python -c "import pyspark; print(f'PySpark {pyspark.__version__}')"

CMD ["python", "-m", "unittest", "discover", "-s", "tests/spark/bronze", "-p", "test_*.py", "-v"]
