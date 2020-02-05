"""
aws.s3
~~~~~~~
Offers 
"""

import boto3


def contents(bucket_name, verbose=False):
    """List contents of an S3 bucket."""
    s3 = boto3.resource('s3')
    print('Contents of {}'.format(bucket_name))
    for obj in s3.Bucket(bucket_name).objects.all():
        print('{date:<32}\t{size:<14}\t{name}'.format(
            date=obj.last_modified.strftime('%Y-%m-%d %H:%M:%S %p %Z'),
            size=obj.size,
            name=obj.key)
        )


def upload(bucket_name, src_path, s3_path, **kargs):
    """Upload file to S3 bucket."""
    s3 = boto3.resource('s3')
    s3.Object(bucket_name, s3_path).upload_file(
        Filename=src_path,
    )


def download(bucket_name, s3_path, dest_path, **kargs):
    """Download file from S3 bucket."""
    s3 = boto3.resource('s3')
    s3.Object(bucket_name, s3_path).download_file(
        Filename=dest_path
    )


def delete(bucket_name, s3_path):
    """Delete file from S3 bucket."""
    s3 = boto3.resource('s3')
    response = s3.Object(bucket_name, s3_path).delete()
    return response['ResponseMetadata']['HTTPStatusCode']
