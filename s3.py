"""S3 utility functions to minimize boto3 interaction.

AWS authentication will be done using your system's
aws config.

It is important to note that fnmatch and glob are
extensively used here to mimic shell patterns when
doing filename matching.

Examples
--------
>>> upload('mramirez-dev', '/home/mramirez/Desktop/Development/*.txt')
Uploading [/home/mramirez/Desktop/Development/*.txt] to S3 [mramirez-dev]

error.txt                        |               63 B | mramirez-dev/error.txt
log.txt                          |             5063 B | mramirez-dev/log.txt
file.txt                         |               20 B | mramirez-dev/file.txt
success.txt                      |             5000 B | mramirez-dev/success.txt

>>> contents('mramirez-dev', '*.txt')
Contents of S3 [mramirez-dev]

2020-02-06 06:43:29 AM UTC       |               63 B | error.txt
2020-02-06 06:43:30 AM UTC       |               20 B | file.txt
2020-02-06 06:43:29 AM UTC       |             5063 B | log.txt
2020-02-06 06:43:30 AM UTC       |             5000 B | success.txt
2020-01-30 08:05:32 AM UTC       |               26 B | test.txt

>>> delete('mramirez-dev', '*.txt')
Deleting [*.txt] from S3 [mramirez-dev]

HTTPStatusCode: 204 | mramirez-dev/error.txt
HTTPStatusCode: 204 | mramirez-dev/file.txt
HTTPStatusCode: 204 | mramirez-dev/log.txt
HTTPStatusCode: 204 | mramirez-dev/success.txt
HTTPStatusCode: 204 | mramirez-dev/test.txt

"""

__author__ = 'mramirez'

import boto3
import glob
import fnmatch
import os


def s3_unix_match(keyname, pattern, matchcase=True):
    """UNIX filename matching for S3 keys.

    Parameters
    ----------
    keyname: str
        S3 key name
    pattern: str
        UNIX style filename pattern

    Returns
    -------
    bool
        True if key string matches UNIX pattern, False otherwise

    Examples
    --------
    >>> s3_unix_match('Testing/', 'Testing')
    True
    >>> s3_unix_match('s3.py', '*.py')
    True
    """
    if matchcase is False:
        return fnmatch.fnmatch(keyname.rstrip('/').split('/')[-1], pattern)
    return fnmatch.fnmatchcase(keyname.rstrip('/').split('/')[-1], pattern)


def objsummary_pprint(objsummary):
    """Pretty prints an ObjectSummary's attributes."""
    obj_date = objsummary.last_modified.strftime('%Y-%m-%d %H:%M:%S %p %Z')
    print(f'{obj_date:<32} | {objsummary.size:>16} B | {objsummary.key}')


def contents(bucket_name, key_pattern=None):
    """List contents of S3 bucket."""
    s3 = boto3.resource('s3')
    print(f'Contents of S3 [{bucket_name}]\n')
    for obj in s3.Bucket(bucket_name).objects.all():
        if key_pattern is None:
            objsummary_pprint(obj)
            continue
        if s3_unix_match(obj.key, key_pattern):
            objsummary_pprint(obj)


def upload(bucket_name, src_pattern, debug=False, **kargs):
    """Upload file(s) to S3 bucket."""
    if debug is True:
        print(f'{"-"*30}\n*---/ DEBUG MODE ON\n{"-"*30}')
    s3 = boto3.resource('s3')
    s3_bucket = s3.Bucket(bucket_name)
    print(f'Uploading [{src_pattern}] to S3 [{bucket_name}]\n')
    for item in glob.iglob(src_pattern):
        filename = os.path.basename(item)
        if debug is False:
            s3_bucket.upload_file(Filename=item, Key=filename)
        print(
            f'{filename:<32} | {os.path.getsize(item):>16} B | {bucket_name}/{filename}')


def download(bucket_name, s3_path, dest_path, debug=False, **kargs):
    """Download file(s) from S3 bucket."""
    if debug is True:
        print(f'{"-"*30}\n*---/ DEBUG MODE ON\n{"-"*30}')
    s3 = boto3.resource('s3')
    s3.Object(bucket_name, s3_path).download_file(
        Filename=dest_path
    )


def delete(bucket_name, del_pattern, debug=False):
    """Delete file from S3 bucket."""
    s3 = boto3.resource('s3')
    s3_bucket = s3.Bucket(bucket_name)
    if debug is True:
        print(f'{"-"*30}\n*---/ DEBUG MODE ON\n{"-"*30}')
    print(f'Deleting [{del_pattern}] from S3 [{bucket_name}]\n')
    for obj in s3_bucket.objects.all():
        if s3_unix_match(obj.key, del_pattern):
            https_code = '204'
            if debug is False:
                response = s3_bucket.Object(obj.key).delete()
                https_code = response['ResponseMetadata']['HTTPStatusCode']
            print(f'HTTPStatusCode: {https_code:5<} | {bucket_name}/{obj.key}')


# Testing
upload('mramirez-dev', '/home/mike/Desktop/Tutorial/*.txt')
# delete('mramirez-dev', '*.txt')
# contents('mramirez-dev', '*.txt')
