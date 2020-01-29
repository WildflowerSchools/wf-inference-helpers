FROM python:3.8-alpine

RUN mkdir /app

WORKDIR /app

RUN pip install retry pika click wildflower-honeycomb-sdk boto3

COPY ./watership /app/watership

CMD ["python", "-m", "watership", "consume-video"]
