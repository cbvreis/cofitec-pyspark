import configparser
import os
import random
import string

basedir = os.path.dirname(os.path.realpath(__file__))

DEBUG = True
config = configparser.ConfigParser()
config.read(f'{basedir}/config.ini')
config.read(f'{basedir}/config.ini')

master = config['CONFIG']['master']
app_name = config['CONFIG']['appName']
config_memory = config['CONFIG']['config_memory']
memory = config['CONFIG']['memory']
config_cores = config['CONFIG']['config_cores']
cores = config['CONFIG']['cores']

#FILE
file_path = config['FILE']['file_path']

# AWS
s3_bucket = config['S3']['s3_bucket']
s3_key = config['S3']['s3_key']
aws_access_key_id = config['S3']['aws_access_key_id']
aws_secret_access_key = config['S3']['aws_secret_access_key']



