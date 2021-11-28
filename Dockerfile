FROM python:3.7-alpine
RUN mkdir -p /usr/src/clientproject
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir google-cloud-dataproc && \
    pip install --no-cache-dir google-cloud-storage
ADD client.py /usr/src/clientproject
WORKDIR /usr/src/clientproject
ENTRYPOINT ["python", "client.py"]