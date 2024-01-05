import json
import os
from typing import Optional
import fnmatch
import boto3
from botocore.exceptions import NoCredentialsError
from utils import heconstants

# Setup S3 client
s3_client = boto3.client('s3', aws_access_key_id=heconstants.AWS_ACCESS_KEY,
                         aws_secret_access_key=heconstants.AWS_SECRET_ACCESS_KEY)


class S3SERVICE:
    def __init__(self):
        self.default_bucket = heconstants.ASR_BUCKET

    def upload_to_s3(self, s3_filename, data, bucket_name: Optional[str] = None, is_json: Optional[bool] = False):
        try:
            if bucket_name is None:
                bucket_name = self.default_bucket
            if is_json:
                data = json.dumps(data).encode('utf-8')
            s3_client.put_object(Bucket=bucket_name, Key=s3_filename, Body=data)
            print(f"Upload Successful: {s3_filename}")
        except FileNotFoundError:
            print("The file was not found")
        except NoCredentialsError:
            print("Credentials not available")

    def get_json_file(self, s3_filename, bucket_name: Optional[str] = None):
        try:
            if bucket_name is None:
                bucket_name = self.default_bucket
            s3_object = s3_client.get_object(Bucket=bucket_name, Key=s3_filename)
            file_content = s3_object['Body'].read().decode('utf-8')
            json_data = json.loads(file_content)
            return json_data
        except FileNotFoundError:
            print("The file was not found")
        except NoCredentialsError:
            print("Credentials not available")

    def get_audio_file(self, s3_filename, bucket_name: Optional[str] = None):
        try:
            if bucket_name is None:
                bucket_name = self.default_bucket
            s3_object = s3_client.get_object(Bucket=bucket_name, Key=s3_filename)
            return s3_object
        except FileNotFoundError:
            print("The file was not found")
        except NoCredentialsError:
            print("Credentials not available")

    def check_file_exists(self, key, bucket_name: Optional[str] = None):
        try:
            if bucket_name is None:
                bucket_name = self.default_bucket
            s3_client.head_object(Bucket=bucket_name, Key=key)
            return True
        except s3_client.exceptions.ClientError:
            return False

    def get_files_matching_pattern(self, pattern, bucket_name: Optional[str] = None):
        json_data_list = []
        try:
            if bucket_name is None:
                bucket_name = self.default_bucket
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
            json_data_list.sort(key=lambda x: x['chunk_no'])
            return json_data_list
        except Exception as exc:
            print(f"Error get_files_matching_pattern file: {exc}")
            return []
        except NoCredentialsError:
            print("Credentials not available")
            return []
        except s3_client.exceptions.ClientError as e:
            print(f"An error occurred: {e}")
            return []

    def get_dirs_matching_pattern(self, pattern, bucket_name: Optional[str] = None):
        dirs_matching_pattern = set()
        try:
            if bucket_name is None:
                bucket_name = self.default_bucket
            # Extract the prefix from the pattern (up to the first wildcard)
            prefix = pattern.split('*')[0]

            # Paginate through results if there are more objects than the max returned in one call
            paginator = s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
                if 'Contents' in page:
                    # Filter the objects whose keys match the pattern and check if they represent directories
                    for obj in page['Contents']:
                        if fnmatch.fnmatch(obj['Key'], pattern):
                            dirname = os.path.dirname(obj['Key'])
                            dirs_matching_pattern.add(dirname)

            # Convert the set of directory names to a list
            dirs_list = list(dirs_matching_pattern)
            return dirs_list
        except Exception as exc:
            print(f"Error get_dirs_matching_pattern: {exc}")
            return []
        except NoCredentialsError:
            print("Credentials not available")
            return []
        except s3_client.exceptions.ClientError as e:
            print(f"An error occurred: {e}")
            return []

    def list_files_in_directory(self, directory, bucket_name: Optional[str] = None):
        try:
            if bucket_name is None:
                bucket_name = self.default_bucket
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=directory)
            return [item['Key'] for item in response.get('Contents', [])]
        except Exception as e:
            print(f"Error listing files: {e}")

    def download_from_s3(self, key, local_path, bucket_name: Optional[str] = None):
        try:
            if bucket_name is None:
                bucket_name = self.default_bucket
            s3_client.download_file(bucket_name, key, local_path)
            print(f"Download Successful: {local_path}")
        except Exception as e:
            print(f"Error downloading file: {e}")


# if __name__ == "__main__":
#     dirs = S3SERVICE().get_dirs_matching_pattern(pattern="carereq*")
#     print(dirs)