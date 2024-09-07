import boto3
import datetime
import os
import time

def get_last_year():
    """Calculate the previous year."""
    return datetime.datetime.now().year - 1

def get_environment_variables():
    """Retrieve environment variables for database name and output location."""
    database = os.environ['DATABASE_NAME']
    output_location = os.environ['OUTPUT_LOCATION']
    return database, output_location

def get_aws_account_id():
    """Retrieve the AWS account ID using STS."""
    sts_client = boto3.client('sts')
    return sts_client.get_caller_identity()['Account']

def construct_query(year):
    """Construct the Athena query using the given year."""
    return f"""
    INSERT INTO new_parquet
    SELECT id,
           date,
           element,
           datavalue,
           mflag,
           qflag,
           sflag,
           obstime,
           substr(date, 1, 4) AS year
    FROM original_csv
    WHERE cast(substr(date, 1, 4) AS bigint) = {year}
    """

def start_athena_query(client, query, database, output_location):
    """Start the Athena query execution and return the query execution ID."""
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': output_location
        }
    )
    return response['QueryExecutionId']

def wait_for_query_to_complete(client, query_execution_id):
    """Wait for the Athena query to complete."""
    while True:
        result = client.get_query_execution(QueryExecutionId=query_execution_id)
        status = result['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            return status
        time.sleep(5)

def run_msck_repair_table(client, database, table, output_location):
    """Run the MSCK REPAIR TABLE command to update partitions."""
    query = f"MSCK REPAIR TABLE {table};"
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': output_location
        }
    )
    query_execution_id = response['QueryExecutionId']
    return wait_for_query_to_complete(client, query_execution_id)

def send_sns_notification(sns_client, sns_topic_arn, subject, message):
    """Send an SNS notification."""
    sns_client.publish(
        TopicArn=sns_topic_arn,
        Subject=subject,
        Message=message
    )

def lambda_handler(event, context):
    last_year=2019
    database, output_location = get_environment_variables()
    query = construct_query(last_year)

    client = boto3.client('athena')
    sns_client = boto3.client('sns')

    aws_account_id = get_aws_account_id()
    aws_region = os.environ['AWS_REGION']

    # Construct SNS topic ARN
    sns_topic_arn = f"arn:aws:sns:{aws_region}:{aws_account_id}:dehtopic"

    # Run the main query
    query_execution_id = start_athena_query(client, query, database, output_location)
    status = wait_for_query_to_complete(client, query_execution_id)

    if status != 'SUCCEEDED':
        send_sns_notification(sns_client, sns_topic_arn, 'Athena Query Failed', f'Query failed with status: {status}')
        return {
            'statusCode': 500,
            'body': f'Query failed with status: {status}'
        }

    # Run MSCK REPAIR TABLE to update partitions
    repair_status = run_msck_repair_table(client, database, 'new_parquet', output_location)

    if repair_status == 'SUCCEEDED':
        send_sns_notification(sns_client, sns_topic_arn, 'Athena Query Succeeded', f'Query succeeded and table repaired: {query_execution_id}')
        return {
            'statusCode': 200,
            'body': f'Query succeeded and table repaired: {query_execution_id}'
        }
    else:
        send_sns_notification(sns_client, sns_topic_arn, 'Table Repair Failed', f'Query succeeded but table repair failed with status: {repair_status}')
        return {
            'statusCode': 500,
            'body': f'Query succeeded but table repair failed with status: {repair_status}'
        }
