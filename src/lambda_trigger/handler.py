import boto3

glue = boto3.client('glue')

def lambda_handler(event, context):
    print(f"Received event: {event}")
    
    response = glue.start_job_run(
        JobName='bronze-to-silver-etl'
    )
    
    print(f"Started Glue job run: {response['JobRunId']}")
    
    return {
        'statusCode': 200,
        'body': f"Glue job triggered: {response['JobRunId']}"
    }