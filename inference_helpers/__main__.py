import json
import logging
import os
import sys
from traceback import print_exc

import click

from inference_helpers.utils import parse_duration, parse_datetime, push_xcom, json_dumps, parse_date, ISO_FORMAT
from inference_helpers.honeycomb import load_file_from_s3, get_assignments, get_datapoint_keys_for_assignment_in_range, get_environment_id, create_inference_execution
from inference_helpers.jobs.prepare import prepare_range_assignment


DATA_ROOT = os.getenv("DATA_ROOT", "/data")

os.makedirs(DATA_ROOT, exist_ok=True)


@click.group()
@click.pass_context
def main(ctx):
    pass


@main.command()
@click.pass_context
@click.option('--environment_name', help='name of environment in honeycomb', required=True)
@click.option('--date', help='day to check assignments at (not implemented yet)', required=False)
def update_assignment_list(ctx, environment_name, date):
    logging.info(f"starting environment listing {environment_name} at {date}(to be implemented)")
    try:
        environment_id = get_environment_id(environment_name)
        assignments = get_assignments(environment_id)
        results = []
        for assignment, device_id in assignments:
            message = {
                "assignment_id": assignment,
                "device_id": device_id,
            }
            results.append(message)
        os.makedirs(os.path.join(DATA_ROOT, environment_name, environment_id), exist_ok=True)
        with open(os.path.join(DATA_ROOT, f"{date}-assignments.json"), 'w') as assignment_manifest:
            assignment_manifest.write(json_dumps(results))
            assignment_manifest.flush()
        logging.info(f"completed {environment_name} at {date} (to be implemented) found {len(results)} assignments")
    except:
        logging.error("a problem with %s when trying to load the assignments", environment_name)


@main.command()
@click.pass_context
@click.option('--environment_name', help='name of environment in honeycomb', required=True)
@click.option('--start', help='starting timestamp or date of the range', required=True)
@click.option('--duration', help='duration of video to prepare', required=False, default="10m")
@click.option('--assignment', help='assignment of video to prepare', required=True)
@click.option('--device', help='device of video to prepare', required=True)
def prepare_assignment_videos(ctx, environment_name, start, duration, assignment, device):
    logging.info("starting prepare for %s starting at %s for %s", environment_name, start, duration)
    environment_id = get_environment_id(environment_name)
    duration_td = parse_duration(duration)
    date = parse_date(start)
    start_dt = parse_datetime(start)
    end_dt = start_dt + duration_td
    prepare_range_assignment(environment_id, assignment, device, start_dt.strftime(ISO_FORMAT), end_dt.strftime(ISO_FORMAT))


if __name__ == '__main__':
    logger = logging.getLogger()

    logger.setLevel(os.getenv("LOG_LEVEL", logging.INFO))
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s - %(message)s'))
    logger.addHandler(handler)
    main()
