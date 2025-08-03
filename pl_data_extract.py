#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import json
import os
import boto3
from datetime import datetime
import requests

def lambda_handler(event, context):
    # TODO implement
    headers = { "X-Auth-Token": "yourtoken" } #Token
    url = "link"  #Link
    response = requests.get(url, headers=headers)
    data = response.json()

    client = boto3.client('s3')

    Bucket_name = 'pl-etl-ishan-project'
    Key = 'raw_data/to_process/'
    Body = data
    filename = 'raw_data_' +str(datetime.now()) + '.json'

    client.put_object(Body=json.dumps(data), 
    Bucket=Bucket_name, 
    Key= Key + filename)


