import logging
import os

import boto3
import honeycomb


HONEYCOMB_URI = os.getenv("HONEYCOMB_URI", "https://honeycomb.api.wildflower-tech.org/graphql")
HONEYCOMB_TOKEN_URI = os.getenv("HONEYCOMB_TOKEN_URI", "https://wildflowerschools.auth0.com/oauth/token")
HONEYCOMB_AUDIENCE = os.getenv("HONEYCOMB_AUDIENCE", "https://honeycomb.api.wildflowerschools.org")
HONEYCOMB_CLIENT_ID = os.getenv("HONEYCOMB_CLIENT_ID")
HONEYCOMB_CLIENT_SECRET = os.getenv("HONEYCOMB_CLIENT_SECRET")

def get_client():
    return honeycomb.HoneycombClient(
        uri=HONEYCOMB_URI,
        client_credentials={
            'token_uri': HONEYCOMB_TOKEN_URI,
            'audience': HONEYCOMB_AUDIENCE,
            'client_id': HONEYCOMB_CLIENT_ID,
            'client_secret': HONEYCOMB_CLIENT_SECRET,
        }
    )


def get_video_details(data_id, honeycomb_client=None):
    if honeycomb_client is None:
        honeycomb_client = get_client()
    result = honeycomb_client.query.query(
        """
        query getDatapoint($data_id: ID!) {
          getDatapoint(data_id: $data_id) {
            data_id
            timestamp
            source {
              ... on Assignment {
                assigned {
                  ... on Device {
                    device_id
                  }
                }
              }
            }
            file {
              key
              bucketName
            }
          }
        }
        """,
        {"data_id": data_id})
    if hasattr(result, "get"):
        return result.get("getDatapoint")
    return None


def load_file_from_s3(key, bucketName, output):
    s3 = boto3.resource('s3')
    logging.info(f"loading {key} from {bucketName} into {output}")
    directory = os.path.dirname(output)
    os.makedirs(directory, exist_ok=True)
    s3.meta.client.download_file(bucketName, key, output)


def get_environment_id(environment_name, honeycomb_client=None):
    if honeycomb_client is None:
        honeycomb_client = get_client()
    environments = honeycomb_client.query.findEnvironment(name=environment_name)
    return environments.data[0].get('environment_id')


def get_assignments(environment_id, honeycomb_client=None):
    if honeycomb_client is None:
        honeycomb_client = get_client()
    result = honeycomb_client.query.query(
        """
        query getEnvironment ($environment_id: ID!) {
          getEnvironment(environment_id: $environment_id) {
            environment_id
            name
            assignments(current: true) {
              assignment_id
              assigned_type
              assigned {
                ... on Device {
                    device_id
                    device_type
                }
              }
            }
          }
        }
        """,
        {"environment_id": environment_id})
    if hasattr(result, "get"):
        assignments = result.get("getEnvironment").get("assignments")
        return [(assignment["assignment_id"], assignment["assigned"]["device_id"]) for assignment in assignments if assignment["assigned_type"] == "DEVICE" and assignment["assigned"]["device_type"].find("CAMERA") > 0]
    else:
        logging.debug(result)
        return []


def get_datapoint_keys_for_assignment_in_range(assignment_id, start, end, honeycomb_client=None):
    if honeycomb_client is None:
        honeycomb_client = get_client()
    query_pages = """
        query searchDatapoints($cursor: String, $assignment_id: String, $start: String, $end: String) {
          searchDatapoints(
            query: { operator: AND, children: [
                { operator: EQ, field: "source", value: $assignment_id },
                { operator: GTE, field: "timestamp", value: $start },
                { operator: LT, field: "timestamp", value: $end },
            ] }
            page: { cursor: $cursor, max: 1000, sort: {field: "timestamp", direction: DESC} }
          ) {
            page_info {
              count
              cursor
            }
            data {
              data_id
              timestamp
              file {
                key
                bucketName
              }
            }
          }
        }
        """
    cursor = ""
    while True:
        page = honeycomb_client.raw_query(query_pages, {"assignment_id": assignment_id, "start": start, "end": end, "cursor": cursor})
        page_info = page.get("searchDatapoints").get("page_info")
        data = page.get("searchDatapoints").get("data")
        cursor = page_info.get("cursor")
        if page_info.get("count") == 0:
            break
        for item in data:
            yield item
