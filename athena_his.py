import boto3
import time
import io
import json
import uuid
from datetime import datetime, date
def lambda_handler(event, context):
    # TODO implement
    to_scrape=5000
    page_size=50
    bucket = "quandata1"
    prefix = "athena_his/"
    s3_client = boto3.client('s3')
    ath = boto3.client('athena')
    paginator = ath.get_paginator("list_query_executions")
    dynamodb = boto3.resource('dynamodb')
    def json_serial(obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        raise TypeError("Type %s not serializable" % type(obj))
    df = []
    break_flag = False
    for workgroup in [w['Name'] for w in ath.list_work_groups()['WorkGroups']]:
        print(f'running {workgroup}')
        i=0
        table = dynamodb.Table('athena_his_sql')
        response = table.get_item(Key={'workgroup': workgroup,})
        curr_query_id = ''
        if  ("Item" in response): curr_query_id = response['Item']['curr_query_id']
        print("get ddb curr_id: " + workgroup + curr_query_id)
        args = {"WorkGroup": workgroup, "PaginationConfig": {"MaxItems": to_scrape, "PageSize": page_size}}
        for page in paginator.paginate(**args):
            query_ids = page['QueryExecutionIds']
            for query_id in query_ids:
                print("query_id:" + query_id)
                if i == 0:
                    table.update_item(
                        Key={'workgroup': workgroup,},
                        UpdateExpression='SET curr_query_id = :val1',
                        ExpressionAttributeValues={':val1': query_id})
                    print("update ddb curr_id: " + query_id)
                if query_id == curr_query_id:
                    break_flag=True
                    break
                query_metadata = ath.get_query_execution(QueryExecutionId=query_id)['QueryExecution']
                df.append(query_metadata)
                i += 1
            if break_flag==True:
                break
            time.sleep(1)
    json_writer = io.BytesIO()
    for record in df:
        line = json.dumps(record, default=json_serial) + "\n"
        json_writer.write(line.encode())
    s3_client.put_object(
        Bucket=bucket,
        Key=prefix + "%s.json" % uuid.uuid4(),
        ContentType='text/json',
        Body=json_writer.getvalue()
