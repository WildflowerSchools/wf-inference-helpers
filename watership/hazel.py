import logging
import os
import json

from watership.fiver import connect_to_rabbit, close, send_message
from watership.honeycomb import get_video_details, load_file_from_s3, get_assignments, get_datapoint_keys_for_assignment_in_range
from watership.jobs.prepare import prepare_range, prepare_range_assignment

__all__ = ["start_consuming"]

DATA_PROCESS_DIRECTORY = os.getenv("DATA_PROCESS_DIRECTORY", "/data/process-temp/")
POSE_QUEUE_NAME = os.getenv("POSE_QUEUE_NAME", "localhost-pose-queue")


def job_processor(ch, method, properties, body):
    msg = json.loads(body)
    job = msg.get("job")
    if job == "load_video" or (job is None and "data_id" in msg):
        # legacy
        data_id = msg.get("data_id")
        output = os.path.join(DATA_PROCESS_DIRECTORY, data_id, f"file.mp4")
        details = video_preloader(data_id, output)
        if details is not None:
            message = {
                "data_id": data_id,
                "device_id": details.get("source", {}).get("assigned", {}).get("device_id", "garbage"),
                "timestamp": details.get("timestamp"),
                "path": output,
                "dataset": "FUTURE",
            }
            send_message(ch, POSE_QUEUE_NAME, json.dumps(message))
    if job == "prepare-range":
        prepare_range(ch, method.routing_key, msg)
    if job == "prepare-range-assignment":
        prepare_range_assignment(ch, method.routing_key, msg)

    ch.basic_ack(delivery_tag=method.delivery_tag)


def video_preloader(data_id, output):
    logging.info("loading video %r" % data_id)
    details = get_video_details(data_id)
    if details is not None:
        bucket = details.get("file").get("bucketName")
        key = details.get("file").get("key")
        load_file_from_s3(key, bucket, output)
        logging.info("file done, sending next task")
    else:
        logging.info(f"datapoint {data_id} is invalid in some way, cannot read")
    return details


def start_consuming(channel, queue_name):
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, on_message_callback=job_processor)
    channel.start_consuming()
