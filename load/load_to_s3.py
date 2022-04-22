import logging
import boto3
from airflow.hooks.base import BaseHook

def upload_to_s3(data_stream, airflow_connection_s3: str,bucket_name_s3: str, path: str) -> None:
    # Function to upload files to S3
    connection = BaseHook.get_connection(airflow_connection_s3)
    extra = connection.extra # This is a getter that returns the extra content of the Airflow connection.
    extra = eval(extra)
    logging.info("Connection information extracted")

    s3_resource = boto3.resource('s3',
            aws_access_key_id= extra.get('aws_access_key_id'),
            aws_secret_access_key= extra.get('aws_secret_access_key'),
            aws_session_token=extra.get('aws_session_token')
        )
    s3_resource.Object(bucket_name_s3, path).put(Body=data_stream)