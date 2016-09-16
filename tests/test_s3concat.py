import hashlib
import random
import string

import boto3
import faker
import pytest
try:
    import ujson as json
except ImportError:
    import json


fake = faker.Factory.create()


def random_chars(n=12):
    return ''.join(
        random.choice(string.ascii_lowercase + string.digits)
        for _ in xrange(n))


def generate_file(size, overshoot=True):
    content = ''
    while 1:
        s = json.dumps({'first_name': fake.name(),
                        'address': fake.address()}) + '\n'
        if len(content) + len(s) > size:
            if overshoot:
                content += s
            break
        content += s
    return content


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


def test_generate_file_overshoot():
    size = 1024**2
    content = generate_file(size, True)
    assert size < len(content)


def test_generate_file_no_overshoot():
    size = 1024**2
    content = generate_file(size, False)
    assert size >= len(content)


def s3concat(bucket, key, content):
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket, Key=key, Body=content)


def test_concat_to_new_object(s3, bucket):
    key = 'newobj'
    content = generate_file(1024)
    h = md5(content)
    s3concat(bucket, key, content)
    resp = s3.get_object(Bucket=bucket, Key=key)
    downloaded = resp['Body'].read()
    assert h == md5(downloaded)
