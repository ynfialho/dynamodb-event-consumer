from common.functions import setting_log
from common import dynamodb_json
from common import constants as cs
import json
import datetime
import boto3 
import pytz

LOGGER = setting_log()

def lambda_handler(event, context):
    LOGGER.info('Starting dynamodb records processing')
    session = boto3.Session(region_name=cs.AWS_REGION)
    firehose_client = session.client('firehose')
    datetime_now = datetime.datetime.now(pytz.timezone('America/Sao_Paulo'))
    firehose_batch_list = list()

    for item in event.get('Records'):
        try:
            crude_record = item.get('dynamodb').get('NewImage')
            # list structure: [service, entity, stream, timestamp]
            entity = item.get('eventSourceARN').split('/')[1]
            firehose_name = cs.FIREHOSE_MAP.get(entity, False)

            if firehose_name:
                normalize_record = dynamodb_json.loads(crude_record)
                normalize_record['dt_ingestion'] = datetime_now.isoformat('T', 'seconds')
                payload_json_lines = json.dumps(normalize_record)+'\n'

                firehose_batch_list.append({'Data': payload_json_lines.encode()})

                LOGGER.info('PUT firehose: {}'.format(firehose_name))
                firehose_client.put_record_batch(
                    DeliveryStreamName=firehose_name,
                    Records=firehose_batch_list)
            else:
                LOGGER.warning('Entity {} not found'.format(entity))
                return False
            
        except Exception as e:
            LOGGER.error(e)
            return False
            
    LOGGER.info('Ending execution')
    return True
    