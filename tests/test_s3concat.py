import hashlib
import random
import string

import boto3
import pytest

from s3concat import s3concat


KB = 1024
MB = KB**2


def random_chars(n=12):
    return ''.join(
        random.choice(string.ascii_lowercase + string.digits)
        for _ in xrange(n))


def generate_file(size):
    chars = string.ascii_letters + string.digits + '+/'
    return ''.join((random.choice(chars) if (i + 1) % 80 else '\n')
                   for i in xrange(size - 1)) + '\n'


def md5(content):
    m = hashlib.md5()
    m.update(content)
    return m.hexdigest()


@pytest.fixture(scope='session')
def s3():
    return boto3.client('s3')


@pytest.fixture
def bucket(request, s3):
    bucket_name = 's3concat-test-' + random_chars()
    s3.create_bucket(Bucket=bucket_name)

    yield bucket_name

    s3.delete_objects(
        Bucket=bucket_name,
        Delete={'Objects': [
            {'Key': rec['Key']} for rec
            in s3.list_objects_v2(Bucket=bucket_name)['Contents']]})
    s3.delete_bucket(Bucket=bucket_name)


@pytest.mark.parametrize('size', [
    100,
    1024,
    1024**2 + 3])
def test_generate_file(size):
    content = generate_file(size)
    assert size == len(content)


@pytest.mark.parametrize('size_source, size_diff', [
    (KB, KB),
    (KB, 5 * MB + KB),
    (5 * MB + KB, KB),
    (5 * MB + KB, 5 * MB + KB)])
def test_concat(s3, bucket, size_source, size_diff):
    key = 'newobj'
    content = generate_file(size_source)
    h = md5(content)

    # concat to a non-existing key creates a new object
    s3concat(bucket, key, content)
    resp = s3.get_object(Bucket=bucket, Key=key)
    downloaded = resp['Body'].read()
    assert h == md5(downloaded)

    # concat to an existing key adds to the object
    diff = generate_file(size_diff)
    h = md5(content + diff)
    s3concat(bucket, key, diff)
    resp = s3.get_object(Bucket=bucket, Key=key)
    downloaded = resp['Body'].read()
    assert h == md5(downloaded)
