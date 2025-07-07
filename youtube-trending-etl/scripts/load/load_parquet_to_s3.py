import boto3
import os
import datetime
from dotenv import load_dotenv

def get_latest_file_in_directory(directory,extension,date_str):
    """
    Get latest files in a directory with a specific extension.
    :param directory: A directory to search for files.
    :param extension: File's extension to look for.
    :param date_str: date string to search for.
    """
    return [
        os.path.join(directory,f)
        for f in os.listdir(directory)
        if f.endswith(extension) and date_str in f
    ]

def load_parquet_to_s3():
    load_dotenv()
    extension = '.parquet'
    date = datetime.date.today().strftime('%Y_%m_%d')

    # S3 connection params
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_bucket = os.getenv('AWS_BUCKET_NAME')
    aws_region = os.getenv('AWS_REGION')

    # S3 prefix and folders path
    folders = {
        'process_videos' : '/home/thangtranquoc/youtube-project/youtube-trending-etl/data/processed/parquet'
    }

    # S3 connection string
    s3 = boto3.client(
        's3',
        aws_access_key_id = aws_access_key_id,
        aws_secret_access_key = aws_secret_access_key,
        region_name = aws_region
    )

    # Upload files upto Prefix
    for prefix,folder in folders.items():
        file_paths = get_latest_file_in_directory(folder,extension,date)
        if file_paths:
            for file_path in file_paths:
                filename = os.path.basename(file_path)
                s3_key = f'{prefix}/{filename}'
                s3.upload_file(file_path,aws_bucket,s3_key)
                print(f'Uploaded: {file_path} to s3://{aws_bucket}/{s3_key}')
            else:
                print(f'No .parquet files were found! for date {date} in {folder}')

load_parquet_to_s3()