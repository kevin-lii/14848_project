FROM python:3.7-alpine
ADD client.py /client/
WORKDIR /client/
CMD ["python", "main.py"]