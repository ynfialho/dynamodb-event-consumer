import logging
from sys import stdout
import os
from datetime import datetime
from common import constants as cs

def setting_log(flag_stdout=True, flag_logfile=False):
    """
    Applies log settings and returns a logging object.
    :flag_stdout: boolean
    :flag_logfile: boolean
    """
    handler_list = list()
    LOGGER = logging.getLogger()

    [LOGGER.removeHandler(h) for h in LOGGER.handlers]

    if flag_logfile:
        path_log = './logs/{}_{:%Y%m%d}.log'.format('log', datetime.now())
        if not os.path.isdir('./logs'):
            os.makedirs('./logs')
        handler_list.append(logging.FileHandler(path_log))

    if flag_stdout:
        handler_list.append(logging.StreamHandler(stdout))
        
    logging.basicConfig(
        level=logging.INFO\
        ,format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'\
        ,handlers=handler_list)    
    return LOGGER

def merge_json(header, footer):
    """
    Merge two jsons
    :header: dict
    :footer: dict
    """
    return {**header, **footer}

def s3_write_object(obj, boto_session, s3_bucket, s3_key):
    """
    Function recived object and write s3 key.
    :obj: str
    :boto_session: boto3 session
    :s3_bucket: str
    :s3_key: str
    """
    s3_resource = boto_session.resource('s3')
    s3_obj = s3_resource.Object(s3_bucket, s3_key)
    s3_obj.put(Body=obj)

def format_hive_path(str_date, path, file_name):
    """
    Formats partitions on the Hive model
    :str_date: str
    :path: str
    :file_name: str
    """
    year, month, day = str_date.split('-')
    return  '{}/year={}/month={}/day={}/{}'.format(path, year, month, day, file_name)