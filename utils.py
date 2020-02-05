import fnmatch


def s3_unix_match(keyname, pattern, matchcase=True):
    """UNIX filename matching for S3 keys.
    :param keyname: S3 key value
    :param pattern: UNIX style filename pattern
    :returns: True if key string matches UNIX pattern, False otherwise
    :Example
    >>> s3_unix_match('Testing/', 'Testing')
    True
    >>> s3_unix_match('s3.py', '*.py')
    True
    """
    if matchcase is False:
        return fnmatch.fnmatch(keyname.rstrip('/').split('/')[-1], pattern)
    return fnmatch.fnmatchcase(keyname.rstrip('/').split('/')[-1], pattern)
