from aws_cdk import (
    Duration,
    Stack,
    # aws_sqs as sqs,
)

# import aws_cdk as core
from aws_cdk.aws_lambda import Function,InlineCode, Runtime
from aws_cdk.aws_iam import Role, ServicePrincipal
from constructs import Construct
import os

class LambdaStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Define your Lambda function code (replace with your actual code)
        my_lambda_code = InlineCode.from_asset("./src")  # Replace with path to your code

        # Get the AWS account ID
        #account_id = boto3.client('sts').get_caller_identity()['Account']
        account_id=os.environ["CDK_DEFAULT_ACCOUNT"]

        # Define development account ID (replace with your actual development account)
        dev_account_id = "030798167757"  # Replace with your development account ID

        # Define environment variables dictionaries
        dev_env_vars = {
            "DATABASE_NAME": "blogdb",
            "OUTPUT_LOCATION": "s3://bluejeans59/athena-query-results/",
        }

        prod_env_vars = {
            "DATABASE_NAME": "blogdb_prd",
            "OUTPUT_LOCATION": "s3://bluejeans59/athena-query-results/",
        }

        # Set environment variables based on account ID
        env_vars = dev_env_vars if account_id == dev_account_id else prod_env_vars

        # Create IAM role for Lambda execution (optional)
        lambda_role = Role(
            self,
            "LambdaRole",
            assumed_by=ServicePrincipal("lambda.amazonaws.com"),
            # Add necessary IAM policies for your Lambda function's requirements
        )

        # Create the Lambda function
        my_lambda = Function(
            self,
            "MyLambda",
            runtime=Runtime.PYTHON_3_9,  # Choose the appropriate runtime
            code=my_lambda_code,
            handler="lambda_function.lambda_handler",  # Replace with your handler function
            environment=env_vars,
            role=Role.from_role_arn(self, 'lambdarole1', role_arn=f'arn:aws:iam::{account_id}:role/AWSLambdaV4'),
            memory_size=128,
            function_name='dehlive-yearly-legal-compliance-job-csv-parquet-paritioned-cdk',
            timeout=Duration.seconds(300)
        )
