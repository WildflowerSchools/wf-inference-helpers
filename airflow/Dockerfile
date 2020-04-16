FROM python:3.8-alpine

RUN mkdir /app

WORKDIR /app

RUN apk add ffmpeg

RUN pip install retry click wildflower-honeycomb-sdk boto3 ffmpeg-python filelock

COPY ./inference_helpers /app/inference_helpers
