FROM ubuntu:20.04

ENV TZ=America/Chicago
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update && \
    apt-get -y install --no-install-recommends default-jdk software-properties-common python3-pip python3.9 python3.9-dev libpq-dev build-essential wget libssl-dev libffi-dev vim && \
    apt-get clean

RUN wget https://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz && \
    tar xvf spark-3.1.1-bin-hadoop3.2.tgz && \
    mv spark-3.1.1-bin-hadoop3.2/ /usr/local/spark && \
    ln -s /usr/local/spark spark

WORKDIR app
COPY . /app

RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.9 2
RUN  update-alternatives --config python3

RUN pip3 install -r requirements.txt

ENV PYSPARK_PYTHON=python3