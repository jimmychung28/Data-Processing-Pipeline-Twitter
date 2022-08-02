 
import datetime
import json
import random
import boto3
import base64
import requests
import os
from dotenv import dotenv_values

config = dotenv_values(".env")
STREAM_NAME = "PUT-S3-5rKi0"


bearer_token = config["BEARER_TOKEN"]
search_url = "https://api.twitter.com/2/tweets/search/recent"
query_params = {'query': '(from:twitterdev -is:retweet) OR #twitterdev', 'tweet.fields': 'author_id'}

# def get_data():
#
#     return {
#         'EVENT_TIME': datetime.datetime.now().isoformat(),
#         'TICKER': random.choice(['AAPL', 'AMZN', 'MSFT', 'INTC', 'TBV']),
#         'PRICE': round(random.random() * 100, 2)}

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2RecentSearchPython"
    return r

def connect_to_endpoint(url, params):
    response = requests.get(url, auth=bearer_oauth, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

# def generate(stream_name, kinesis_client):
#     while True:
#         data = get_data()
#         # print(json.dumps(data))
#         kinesis_client.put_record(
#             DeliveryStreamName=stream_name,
#             Record={'Data':json.dumps(data)})
#             # PartitionKey="partitionkey")


if __name__ == '__main__':
    print(bearer_token)
    json_response = connect_to_endpoint(search_url, query_params)
    print(json.loads(json.dumps(json_response, indent=4, sort_keys=True))['data'])
    boto3.client('firehose').put_record(
            DeliveryStreamName=STREAM_NAME,
            Record={'Data':json.dumps(json.loads(json.dumps(json_response, indent=4, sort_keys=True))['data'])})
                # PartitionKey="partitionkey")
    # generate(STREAM_NAME, boto3.client('firehose'))
