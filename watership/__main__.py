import json
import logging
import os
import sys
from traceback import print_exc

import click

from watership.fiver import connect_to_rabbit, close, send_message
from watership.hazel import start_consuming


RABBIT_HOST = os.getenv("RABBIT_HOST", "localhost")
VIDEO_QUEUE_NAME = os.getenv("VIDEO_QUEUE_NAME", "localhost-video-queue")
POSE_QUEUE_NAME = os.getenv("POSE_QUEUE_NAME", "localhost-pose-queue")


@click.group()
@click.pass_context
def main(ctx):
    pass


@main.command()
@click.pass_context
@click.option('--rabbitmq', help='hostname for rabbitmq', required=False)
@click.option('--queue', help='queue for rabbitmq', required=False)
def consume_video(ctx, rabbitmq=None, queue=None):
    host = rabbitmq if rabbitmq is not None else RABBIT_HOST
    que = queue if queue is not None else VIDEO_QUEUE_NAME
    print(f"attempting to connect to {host}")
    channel = connect_to_rabbit(host, que)
    try:
        print("start consuming")
        start_consuming(channel, que)
    except KeyboardInterrupt:
        close(channel)
    except Exception as e:
        print_exc()
        close(channel)
        sys.exit(-1)


@main.command()
@click.pass_context
@click.option('--data-id', help='id of datapoint in honeycomb', required=True)
@click.option('--rabbitmq', help='hostname for rabbitmq', required=False)
@click.option('--queue', help='queue for rabbitmq', required=False)
def queue_video(ctx, data_id, rabbitmq=None, queue=None):
    host = rabbitmq if rabbitmq is not None else RABBIT_HOST
    que = queue if queue is not None else VIDEO_QUEUE_NAME
    print(f"attempting to connect to {host}")
    channel = connect_to_rabbit(host, que)
    send_message(channel, que, json.dumps({"data_id": data_id}))
    close(channel)


@main.command()
@click.pass_context
@click.option('--data-ids', help='comma separated list of datapoints in honeycomb', required=True)
@click.option('--rabbitmq', help='hostname for rabbitmq', required=False)
@click.option('--queue', help='queue for rabbitmq', required=False)
def queue_videos(ctx, data_ids, rabbitmq=None, queue=None):
    host = rabbitmq if rabbitmq is not None else RABBIT_HOST
    que = queue if queue is not None else VIDEO_QUEUE_NAME
    print(f"attempting to connect to {host}")
    channel = connect_to_rabbit(host, que)
    for data_id in data_ids.split(','):
        send_message(channel, que, json.dumps({"data_id": data_id.strip()}))
    close(channel)


@main.command()
@click.pass_context
@click.option('--data-id', help='id of datapoint in honeycomb', required=True)
@click.option('--device-id', help='id of device in honeycomb', required=True)
@click.option('--timestamp', help='starting timestamp of the video', required=True)
@click.option('--path', help='path of video on data volume', required=True)
@click.option('--dataset', help='label on system that video was downloaded to', required=True)
@click.option('--rabbitmq', help='hostname for rabbitmq', required=False)
@click.option('--queue', help='queue for rabbitmq', required=False)
def queue_posetracking(ctx, data_id, device_id, timestamp, path, dataset, rabbitmq=None, queue=None):
    host = rabbitmq if rabbitmq is not None else RABBIT_HOST
    que = queue if queue is not None else POSE_QUEUE_NAME
    print(f"attempting to connect to {host}")
    channel = connect_to_rabbit(host, que)
    message = {
        "data_id": data_id,
        "device_id": device_id,
        "timestamp": timestamp,
        "path": path,
        "dataset": dataset,
    }
    send_message(channel, que, json.dumps(message))
    close(channel)


@main.command()
@click.pass_context
@click.option('--environment_name', help='name of environment in honeycomb', required=True)
@click.option('--start', help='id of device in honeycomb', required=True)
@click.option('--end', help='starting timestamp of the video', required=True)
@click.option('--rabbitmq', help='hostname for rabbitmq', required=False)
@click.option('--queue', help='queue for rabbitmq', required=False)
def queue_prepare_day(ctx, environment_name, start, end, rabbitmq=None, queue=None):
    host = rabbitmq if rabbitmq is not None else RABBIT_HOST
    que = queue if queue is not None else VIDEO_QUEUE_NAME
    print(f"attempting to connect to {host}")
    channel = connect_to_rabbit(host, que)
    message = {
        "job": "prepare-range",
        "environment_name": environment_name,
        "start": start,
        "end": end,
    }
    send_message(channel, que, json.dumps(message))
    close(channel)


if __name__ == '__main__':
    logger = logging.getLogger()

    logger.setLevel(os.getenv("LOG_LEVEL", logging.INFO))
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s - %(message)s'))
    logger.addHandler(handler)
    main()
