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
    print(f"loading {key} from {bucketName} into {output}")
    directory = os.path.dirname(output)
    os.makedirs(directory, exist_ok=True)
    s3.meta.client.download_file(bucketName, key, output)