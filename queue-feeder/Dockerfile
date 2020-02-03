FROM python:3.8-alpine

RUN mkdir /app

WORKDIR /app

RUN apk add ffmpeg

RUN pip install retry pika click wildflower-honeycomb-sdk boto3 ffmpeg-python

COPY ./watership /app/watership

ENTRYPOINT ["python", "-m", "watership", "queue-prepare-day"]
