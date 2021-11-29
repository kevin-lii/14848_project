FROM python:3.7-slim
RUN mkdir -p /usr/src/clientproject
RUN apt-get update && apt-get install build-essential -y
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir google-cloud-dataproc && \
    pip install --no-cache-dir google-cloud-storage
ADD client.py /usr/src/clientproject
COPY shakespeare.tar.gz /usr/src/clientproject
ADD service_account.json /usr/src/clientproject
WORKDIR /usr/src/clientproject
ENTRYPOINT ["python", "client.py"]