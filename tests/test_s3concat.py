import hashlib
import random
import string

import boto3
import faker
import pytest

from s3concat import s3concat


fake = faker.Factory.create()


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


def test_concat_small(s3, bucket):
    key = 'newobj'
    content = generate_file(1024)
    h = md5(content)

    # concat to a non-existing key creates a new object
    s3concat(bucket, key, content)
    resp = s3.get_object(Bucket=bucket, Key=key)
    downloaded = resp['Body'].read()
    assert h == md5(downloaded)
    print h, len(downloaded)

    # concat to an existing key adds to the object
    diff = generate_file(1024)
    h = md5(content + diff)
    s3concat(bucket, key, diff)
    resp = s3.get_object(Bucket=bucket, Key=key)
    downloaded = resp['Body'].read()
    assert h == md5(downloaded)
    print h, len(downloaded)


def test_concat_to_big_existing_object(s3, bucket):
    key = 'bigobj'
    content = generate_file(5 * 1024**2)
    s3.put_object(Bucket=bucket, Key=key, Body=content)

    diff = generate_file(1024)
    h = md5(content + diff)
    s3concat(bucket, key, diff)
    resp = s3.get_object(Bucket=bucket, Key=key)
    assert h == md5(resp['Body'].read())
