FROM python:3.11-slim-bookworm

# Spark needs a JDK; 17 is supported by Spark 3.5
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

# Path for amd64 (most Windows PCs). On ARM machines use java-17-openjdk-arm64
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /workspace
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt