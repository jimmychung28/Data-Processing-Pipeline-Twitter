# First fetch credentials from environment defaults
# If you can get this far you probably know how to tailor them
# For your particular situation. Otherwise SO is a safe bet :)
import boto3
import json
import urllib
import requests


def lambda_handler(event, context):
    credentials = boto3.Session().get_credentials()
    region = 'us-east-1'  # for example
    # auth = AWSV4SignerAuth(credentials, region)

    # Now set up the AWS 'Signer'
    from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
    auth = AWSV4SignerAuth(credentials, region)

    # And finally the OpenSearch client
    host = "search-twitter-rx7ojk25y7q6t6wjulmcnfoynu.us-east-1.es.amazonaws.com"  # fill in your hostname (minus the https://) here
    client = OpenSearch(
        hosts=[{'host': host, 'port': 443}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    response = boto3.client('s3').get_object(Bucket=bucket, Key=key)
    text = response["Body"].read().decode()
    data = json.loads(text)

    # document1 = {
    #     "title": "Moneyball",
    #     "director": "Bennett Miller",
    #     "year": "2011"
    # }

    # document2 = {
    #     "title": "Apollo 13",
    #     "director": "Richie Cunningham",
    #     "year": "1994"
    # }

    # data = [document1, document2]

    my_index7 = 'my_index7'
    print("hellocat")
    try:
        response = client.indices.create(my_index7)
        print('\nCreating index:')
        print(response)
    except Exception as e:
        # If, for example, my_index already exists, do not much!
        print(e)

    action = {
        "index": {
            "_index": my_index7
        }
    }
    print(payload_constructor(data, action))
    response = client.bulk(body=payload_constructor(data, action), index=my_index7)
    print(response)
    print("china")


def payload_constructor(data, action):
    # "All my own work"

    action_string = json.dumps(action) + "\n"

    payload_string = ""

    for datum in data:
        payload_string += action_string
        this_line = json.dumps(datum) + "\n"
        payload_string += this_line
    return payload_string



