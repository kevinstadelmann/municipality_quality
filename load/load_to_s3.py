import logging
import boto3

from municipality_quality.extraction import extract_from_source

def upload_to_s3(url: str, encoding: str, s3_bucket_name: str, path: str) -> None:
    # Function to upload files to S3
    f = extract_from_source.download_file(url, encoding)
    logging.info("Upload file start")
    s3_resource = boto3.resource('s3',
            aws_access_key_id='ASIAUTZK4H75FSCYXO6C',
            aws_secret_access_key='BzQQxnAy48+yngezs2Mk6upOuMI0Q91Dh/DA9xXd',
            aws_session_token='FwoGZXIvYXdzEA4aDKE4Bra+JEAZ+ip3iyK7ARVt5iXEj2N3+n0PUGvzLt7O0RQu9n+97QACkIvZKTUzB+/k7hw1mRmXHuNQYm3uM1XTfKJ80bHpEm2C6fm9Mxg6xrcBuLllA0jP4l/WSgXdIZ0nFEi2u7AJZgOJNgR3S50hnDe50VMsq4J5kHFLRaxZhhRzt3WT6OWfKQJVNBqzIxlF9pNIlmzcdGDGB4pmA2R9c9v7mHmvrmdP7sDMfnukQq1rbqlrGN4uOipiQLxLcvHs+H9vk5ZptGwo08qykgYyLWrlB+uK9I53DcIdSIP0OPYQC/mEqvbnTdDl0mDuC4w703pDkmGFzKJTXiVcUw=='
        )
    s3_resource.Object(s3_bucket_name, path).put(Body=f)
    logging.info("Upload file end")