FROM python:3.8-buster

RUN mkdir /app

RUN apt update && apt install -y curl
RUN mkdir /build
RUN curl "https://repo.anaconda.com/archive/Anaconda3-2020.02-Linux-x86_64.sh" > /build/conda-install.sh
RUN bash /build/conda-install.sh -b -p /anaconda
RUN /anaconda/bin/conda install numpy pyyaml setuptools cmake cffi typing
RUN pip3 install pyyaml numpy


WORKDIR /app

RUN apt install -y ffmpeg

RUN pip install retry click wildflower-honeycomb-sdk boto3 ffmpeg-python filelock

COPY ./inference_helpers /app/inference_helpers
