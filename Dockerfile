FROM python:3.7-alpine
RUN mkdir -p /usr/src/clientproject
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir requests
ADD client.py /usr/src/clientproject
WORKDIR /usr/src/clientproject
ENTRYPOINT ["python", "client.py"]