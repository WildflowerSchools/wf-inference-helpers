import hashlib
import json
import logging
import os

import ffmpeg

from watership.fiver import send_message
from watership.honeycomb import load_file_from_s3, get_assignments, get_datapoint_keys_for_assignment_in_range, get_environment_id


DATA_PROCESS_DIRECTORY = os.getenv("DATA_PROCESS_DIRECTORY", "/data/process-temp/")


def prepare_range(ch, que, msg):
    name = msg.get("environment_name")
    if name:
        environment_id = get_environment_id(name)
        assignments = get_assignments(environment_id)
        for assignment in assignments:
            message = {
                "job": "prepare-range-assignment",
                "environment_name": name,
                "environment_id": environment_id,
                "start": msg.get("start"),
                "end": msg.get("end"),
                "assignment_id": assignment,
            }
            send_message(ch, que, json.dumps(message))


def prepare_range_assignment(ch, que, msg):
    assignment_id = msg.get("assignment_id")
    environment_id = msg.get("environment_id")
    start = msg.get("start")
    end = msg.get("end")
    hasher = hashlib.sha1()
    hasher.update(start.encode('utf8'))
    hasher.update(end.encode('utf8'))
    ppath = os.path.join(DATA_PROCESS_DIRECTORY, environment_id, assignment_id, hasher.hexdigest())
    output_path = os.path.join(ppath, "output.mp4")
    if assignment_id:
        datapoints = list(get_datapoint_keys_for_assignment_in_range(assignment_id, start, end))
        if len(datapoints):
            paths = []
            for datum in datapoints:
                data_id = datum.get("data_id")
                name = f"{data_id}.mp4"
                timestamp = datum.get("timestamp")
                output = os.path.join(ppath, name)
                bucket = datum.get("file").get("bucketName")
                key = datum.get("file").get("key")
                if not os.path.exists(output):
                    load_file_from_s3(key, bucket, output)
                else:
                    logging.info("file (%s) already exists", output)
                paths.append({"name": name, "timestamp": timestamp})
            files_path = os.path.join(ppath, "files.txt")
            with open(files_path, 'w') as fp:
                for obj in sorted(paths, key=lambda d: d.get("timestamp")):
                    fp.write("file \'")
                    fp.write(obj['name'])
                    fp.write("\'\n")
                fp.flush()
            concat_videos(files_path, output_path)


def concat_videos(input_path, output_path):
    # TODO handle timescale issues due to missing chunks or replaced chunks
    if not os.path.exists(output_path):
        files = ffmpeg.input(input_path, format='concat', safe=0)
        files.output(output_path, c="copy").run()
    else:
        logging.info("concatenated video already exists")
