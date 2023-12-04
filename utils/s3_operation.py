import json
from typing import Optional
import fnmatch
import boto3
from botocore.exceptions import NoCredentialsError

# Setup S3 client
s3_client = boto3.client('s3')


class S3SERVICE:

    def upload_to_s3(self, s3_filename, data, bucket_name: Optional[str] = None, is_json: Optional[bool] = False):
        try:
            if bucket_name is None:
                bucket_name = "healiom-asr"
            if is_json:
                data = json.dumps(data).encode('utf-8')
            s3_client.put_object(Bucket=bucket_name, Key=s3_filename, Body=data)
            print(f"Upload Successful: {s3_filename}")
        except FileNotFoundError:
            print("The file was not found")
        except NoCredentialsError:
            print("Credentials not available")

    def get_audio_file(self, s3_filename, bucket_name: Optional[str] = None):
        try:
            if bucket_name is None:
                bucket_name = "healiom-asr"
            s3_object = s3_client.get_object(Bucket=bucket_name, Key=s3_filename)
            return s3_object
        except FileNotFoundError:
            print("The file was not found")
        except NoCredentialsError:
            print("Credentials not available")

    def check_file_exists(self, key, bucket_name: Optional[str] = None):
        try:
            if bucket_name is None:
                bucket_name = "healiom-asr"
            s3_client.head_object(Bucket=bucket_name, Key=key)
            return True
        except s3_client.exceptions.ClientError:
            return False

    def get_files_matching_pattern(self, pattern, bucket_name: Optional[str] = None):
        json_data_list = []
        try:
            print("getting list of files")
            if bucket_name is None:
                bucket_name = "healiom-asr"
            # Extract the prefix from the pattern (up to the first wildcard)
            prefix = pattern.split('*')[0]

            # Paginate through results if there are more files than the max returned in one call
            paginator = s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
                if 'Contents' in page:
                    # Filter the objects whose keys match the pattern and read each JSON file
                    for obj in page['Contents']:
                        if fnmatch.fnmatch(obj['Key'], pattern):
                            try:
                                response = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])
                                file_content = response['Body'].read().decode('utf-8')
                                json_data = json.loads(file_content)
                                json_data_list.append(json_data)
                            except NoCredentialsError:
                                print("Credentials not available for file:", obj['Key'])
                            except s3_client.exceptions.ClientError as e:
                                print(f"An error occurred with file {obj['Key']}: {e}")

            return json_data_list
        except NoCredentialsError:
            print("Credentials not available")
        except s3_client.exceptions.ClientError as e:
            print(f"An error occurred: {e}")
            return []

    def list_files_in_directory(self, directory, bucket_name: Optional[str] = None):
        if bucket_name is None:
            bucket_name = "healiom-asr"
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=directory)
        return [item['Key'] for item in response.get('Contents', [])]

    def download_from_s3(self, key, local_path, bucket_name: Optional[str] = None):
        try:
            if bucket_name is None:
                bucket_name = "healiom-asr"
            s3_client.download_file(bucket_name, key, local_path)
            print(f"Download Successful: {local_path}")
        except Exception as e:
            print(f"Error downloading file: {e}")
