from os import environ as env

AWS_REGION = env.get('AWS_REGION', 'us-east-1')


FIREHOSE_MAP = {
    'Example': 'firehose-example'
}