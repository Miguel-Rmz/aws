"""
aws.s3
~~~~~~~
Offers 
"""

import boto3
import glob
import utils


def contents(bucket_name, key_pattern=None):
    """List contents of S3 bucket."""
    s3 = boto3.resource('s3')

    def output(obj):
        print('{date:<32}{size:<14}{name}'.format(
            date=obj.last_modified.strftime('%Y-%m-%d %H:%M:%S %p %Z'),
            size=obj.size,
            name=obj.key)
        )
    print('Contents of S3 <{}>'.format(bucket_name))
    for obj in s3.Bucket(bucket_name).objects.all():
        if key_pattern is None:
            output(obj)
            continue
        if utils.s3_unix_match(obj.key, key_pattern):
            output(obj)


def upload(bucket_name, src_pattern, debug=False, **kargs):
    """Upload file(s) to S3 bucket."""
    if debug is True:
        print('## DEBUG MODE ON')
        print(f'Uploading <{src_pattern}> to S3 <{bucket_name}>')
        for item in glob.iglob(src_pattern):
            print(f'{item} => {bucket_name}/{item}')
        return
    s3 = boto3.resource('s3')
    s3_bucket = s3.Bucket(bucket_name)
    print(f'Uploading <{src_pattern}> to S3 <{bucket_name}>')
    for item in glob.iglob(src_pattern):
        s3_bucket.upload_file(Filename=item, Key=item)
        print(f'{item} => {bucket_name}/{item}')


def download(bucket_name, s3_path, dest_path, **kargs):
    """Download file from S3 bucket."""
    s3 = boto3.resource('s3')
    s3.Object(bucket_name, s3_path).download_file(
        Filename=dest_path
    )


def delete(bucket_name, del_pattern, debug=False):
    """Delete file from S3 bucket."""
    s3 = boto3.resource('s3')
    s3_bucket = s3.Bucket(bucket_name)
    if debug is True:
        print('## DEBUG MODE ON')
        print(f'Deleting <{del_pattern}> from S3 <{bucket_name}>')
        for obj in s3_bucket.objects.all():
            if utils.s3_unix_match(obj.key, del_pattern):
                print(f'{bucket_name}/{obj.key}')
        return
    print(f'Deleting <{del_pattern}> from S3 <{bucket_name}>')
    for obj in s3_bucket.objects.all():
        if utils.s3_unix_match(obj.key, del_pattern):
            response = s3_bucket.Object(obj.key).delete()
            https_code = response['ResponseMetadata']['HTTPStatusCode']
            print(f'{bucket_name}/{obj.key} | response: {https_code}')


# Testing
# upload('mramirez-dev', '*.py')
# delete('mramirez-dev', '*.py')
contents('mramirez-dev', '*.txt')
