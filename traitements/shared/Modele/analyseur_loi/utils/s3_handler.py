import boto3
import json
import pandas as pd
from retrying import retry
from utils.logger import setup_logger

logger = setup_logger(__name__)

class S3Handler:
    def __init__(self, bucket_name, base_path=""):
        self.s3_client = boto3.client('s3')
        self.bucket_name = bucket_name
        self.base_path = base_path.rstrip('/')
    
    @retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000)
    def read_json(self, key):
        try:
            full_key = f"{self.base_path}/{key}" if self.base_path else key
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=full_key)
            return json.loads(response['Body'].read().decode('utf-8'))
        except Exception as e:
            logger.error(f"Failed to read JSON from S3: {key}", extra={'extra_data': {'error': str(e)}})
            raise
    
    @retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000)
    def read_csv(self, key):
        try:
            full_key = f"{self.base_path}/{key}" if self.base_path else key
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=full_key)
            return pd.read_csv(response['Body'])
        except Exception as e:
            logger.error(f"Failed to read CSV from S3: {key}", extra={'extra_data': {'error': str(e)}})
            raise
    
    @retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000)
    def write_json(self, key, data):
        try:
            full_key = f"{self.base_path}/{key}" if self.base_path else key
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=full_key,
                Body=json.dumps(data, indent=2),
                ContentType='application/json'
            )
            logger.info(f"Successfully wrote JSON to S3: {key}")
        except Exception as e:
            logger.error(f"Failed to write JSON to S3: {key}", extra={'extra_data': {'error': str(e)}})
            raise
    
    @retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000)
    def write_csv(self, key, dataframe):
        try:
            full_key = f"{self.base_path}/{key}" if self.base_path else key
            csv_buffer = dataframe.to_csv(index=False)
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=full_key,
                Body=csv_buffer,
                ContentType='text/csv'
            )
            logger.info(f"Successfully wrote CSV to S3: {key}")
        except Exception as e:
            logger.error(f"Failed to write CSV to S3: {key}", extra={'extra_data': {'error': str(e)}})
            raise
    
    def list_objects(self, prefix=""):
        try:
            full_prefix = f"{self.base_path}/{prefix}" if self.base_path else prefix
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=full_prefix)
            return [obj['Key'] for obj in response.get('Contents', [])]
        except Exception as e:
            logger.error(f"Failed to list objects from S3: {prefix}", extra={'extra_data': {'error': str(e)}})
            raise