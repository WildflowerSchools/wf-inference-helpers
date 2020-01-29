import os
import json

from watership.fiver import connect_to_rabbit, close, send_message
from watership.honeycomb import get_video_details, load_file_from_s3


__all__ = ["start_consuming"]

DATA_PROCESS_DIRECTORY = os.getenv("DATA_PROCESS_DIRECTORY", "/data/process-temp/")
POSE_QUEUE_NAME = os.getenv("POSE_QUEUE_NAME", "localhost-pose-queue")


def video_preloader(ch, method, properties, body):
    print("=" * 80)
    msg = json.loads(body)
    data_id = msg.get("data_id")
    print("Received %r" % data_id)
    details = get_video_details(data_id)
    if details is not None:
        bucket = details.get("file").get("bucketName")
        key = details.get("file").get("key")
        output = os.path.join(DATA_PROCESS_DIRECTORY, data_id, f"file.{key.split('.')[-1]}")
        load_file_from_s3(key, bucket, output)
        print("file done, sending next task")
        message = {
            "data_id": data_id,
            "device_id": details.get("source", {}).get("assigned", {}).get("device_id", "garbage"),
            "timestamp": details.get("timestamp"),
            "path": output,
            "dataset": "FUTURE",
        }
        send_message(ch, que, json.dumps(message))
    else:
        print(f"datapoint {data_id} is invaid in some way, cannot read")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def start_consuming(channel, queue_name):
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, on_message_callback=video_preloader)
    channel.start_consuming()
