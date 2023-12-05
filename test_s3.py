import fnmatch
import json
from typing import Optional

import boto3
from botocore.exceptions import NoCredentialsError

from utils import heconstants


def get_files_matching_pattern(self, pattern, bucket_name: Optional[str] = None):
    print("getting list of files")
    json_data_list = []
    credentials = {
        'aws_access_key_id': heconstants.AWS_ACCESS_KEY,
        'aws_secret_access_key': heconstants.AWS_SECRET_ACCESS_KEY
    }
    s3_client = boto3.client('s3', 'us-east-2', **credentials)
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
    except Exception as exc:
        self.logger.error(str(exc))
        return []
    except NoCredentialsError:
        print("Credentials not available")
    except s3_client.exceptions.ClientError as e:
        print(f"An error occurred: {e}")
        return []

if __name__ == "__main__":
    get_files_matching_pattern()