import hashlib
import json
import logging
import os

import ffmpeg
from filelock import Timeout, FileLock

from inference_helpers.utils import ts2path
from inference_helpers.honeycomb import load_file_from_s3, get_assignments, get_datapoint_keys_for_assignment_in_range, get_environment_id, create_inference_execution

DATA_PROCESS_DIRECTORY = os.getenv("DATA_PROCESS_DIRECTORY", "/data/prepared/")


def prepare_range_assignment(environment_id, assignment_id, device_id, start, end):
    ppath = os.path.join(DATA_PROCESS_DIRECTORY, environment_id, assignment_id)
    hasher = hashlib.sha1()
    hasher.update(start.encode('utf8'))
    hasher.update(end.encode('utf8'))
    state_id = hasher.hexdigest()
    state_path = os.path.join(ppath, f"{state_id}.json")
    manifest = {
        "environment_id": environment_id,
        "assignment_id": assignment_id,
        "device_id": device_id,
        "start": start,
        "end": end,
        "paths": []
    }
    os.makedirs(os.path.dirname(state_path), exist_ok=True)
    lock = FileLock(state_path + "-lock", timeout=20)
    try:
        with lock:
            logging.info("checking for saved state")
            logging.info("querying datapoints")
            datapoints = list(get_datapoint_keys_for_assignment_in_range(assignment_id, start, end))
            logging.info(f"found { len(datapoints) } datapoints")
            if len(datapoints):
                for datum in datapoints:
                    data_id = datum.get("data_id")
                    timestamp = datum.get("timestamp")
                    output = os.path.join(ppath, f"{ts2path(timestamp)}.mp4")
                    logging.info(f"{data_id} from {timestamp} is going to {output}")
                    bucket = datum.get("file").get("bucketName")
                    key = datum.get("file").get("key")
                    if not os.path.exists(output):
                        load_file_from_s3(key, bucket, output)
                    else:
                        logging.info("file (%s) already exists", output)
                    manifest['paths'].append({"data_id": data_id, "timestamp": timestamp, "video": output})
                    write_state(state_path, manifest)
            write_state(state_path, manifest)
    except:
        logging.error("an error occured")
    finally:
        lock.release()


def write_state(path, state):
    with open(path, 'w') as writer:
        json.dump(state, writer)
        writer.flush()
