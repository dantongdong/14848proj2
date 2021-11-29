FROM python:3.8

FROM google/cloud-sdk:latest

ADD main.py .

ADD credential.json .

RUN pip install google-cloud-storage

RUN curl -sSL https://sdk.cloud.google.com | bash

CMD ["python", "./main.py"]