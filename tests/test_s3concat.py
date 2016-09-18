# -*- coding: utf-8 -*-
import hashlib
import random
import string
from collections import defaultdict

import pytest

from s3concat import resources
from s3concat import s3concat
from s3concat import s3concat_content
from s3concat.s3concat import _get_object_info


KB = 1024
MB = KB**2


def random_chars(n=12):
    return ''.join(
        random.choice(string.ascii_lowercase + string.digits)
        for _ in xrange(n))


def md5(content):
    return hashlib.md5(content).hexdigest()


def generate_file(size):
    chars = string.ascii_letters + string.digits + '+/'
    return ''.join((random.choice(chars) if (i + 1) % 80 else '\n')
                   for i in xrange(size - 1)) + '\n'


@pytest.mark.parametrize('size', [100, 1024, 1024**2 + 3])
def test_generate_file(size):
    assert size == len(generate_file(size))


@pytest.fixture(scope='session')
def s3():
    return resources.s3


@pytest.fixture(scope='class')
def buckets(request, s3):
    buckets = []
    for _ in xrange(2):
        bucket = 's3concat-test-' + random_chars()
        s3.create_bucket(Bucket=bucket)
        buckets.append(bucket)

    yield buckets

    for bucket in buckets:
        resp = s3.list_objects_v2(Bucket=bucket)
        if 'Contents' in resp:
            s3.delete_objects(
                Bucket=bucket,
                Delete={'Objects': [
                    {'Key': rec['Key']} for rec in resp['Contents']]})
        s3.delete_bucket(Bucket=bucket)


@pytest.mark.skip
@pytest.mark.usefixtures('env')
class TestS3ConcatContent(object):

    @pytest.mark.parametrize('size_source, size_diff', [
        (KB, KB),
        (KB, 5 * MB + KB),
        (5 * MB + KB, KB),
        (5 * MB + KB, 5 * MB + KB)])
    def test_s3concat_content(self, s3, buckets, size_source, size_diff):
        bucket = buckets[0]
        key = 'newobj'
        content = generate_file(size_source)
        h = md5(content)

        # concat to a non-existing key creates a new object
        s3concat_content(bucket, key, content)
        resp = s3.get_object(Bucket=bucket, Key=key)
        downloaded = resp['Body'].read()
        assert h == md5(downloaded)

        # concat to an existing key adds to the object
        diff = generate_file(size_diff)
        h = md5(content + diff)
        s3concat_content(bucket, key, diff)
        resp = s3.get_object(Bucket=bucket, Key=key)
        downloaded = resp['Body'].read()
        assert h == md5(downloaded)


@pytest.fixture(scope='class')
def s3concat_env(request, s3, buckets):
    objs = defaultdict(dict)

    bucket = buckets[0]
    for size in (3 * MB, 5 * MB, 7 * MB):
        key = str(size)
        content = generate_file(size)
        objs[bucket][key] = content
        s3.put_object(Bucket=bucket, Key=key, Body=content)

    bucket = buckets[1]
    for size in (1 * KB, 10 * KB, 100 * KB):
        key = str(size)
        content = generate_file(size)
        objs[bucket][key] = content
        s3.put_object(Bucket=bucket, Key=key, Body=content)

    request.cls.s3 = s3
    request.cls.buckets = buckets
    request.cls.env = {'objects': objs}

    yield


@pytest.mark.usefixtures('s3concat_env')
class TestS3Concat(object):

    @classmethod
    def to_url(cls, bucket_number, size):
        return 's3://' + cls.buckets[bucket_number] + '/' + str(size)

    @pytest.mark.parametrize('concat_args', [
        ((0, 8*MB+1*KB), (0, 3*MB), (0, 5*MB), (1, 1*KB)),
        ((1, 11*KB), (1, 1*KB), (1, 10*KB)),
        ((1, 12*MB), (0, 5*MB), (0, 7*MB)),
        ((1, 112*KB), (1, 1*KB), (1, 10*KB), (1, 1*KB), (1, 100*KB)),
        ((1, 7*MB+111*KB), (1, 1*KB), (1, 10*KB), (1, 7*MB), (1, 100*KB)),
    ])
    def test_s3concat(self, concat_args):
        urls = [self.to_url(bucket_number, size)
                for bucket_number, size in concat_args]

        content = ''
        for bucket_number, size in concat_args:
            objs = self.env['objects'][self.buckets[bucket_number]]
            if str(size) in objs:
                content += objs[str(size)]

        h = md5(content)

        s3concat(*urls)

        bucket_number, size = concat_args[0]

        resp = self.s3.get_object(
            Bucket=self.buckets[bucket_number], Key=str(size))
        downloaded = resp['Body'].read()
        assert h == md5(downloaded)

    def test_s3concat_remove_orig(self):
        bucket = self.buckets[0]
        self.s3.put_object(Bucket=bucket, Key='1', Body='a')
        self.s3.put_object(Bucket=bucket, Key='2', Body='ab')

        urls = ['s3://{bucket}/3'.format(bucket=bucket),
                's3://{bucket}/1'.format(bucket=bucket),
                's3://{bucket}/2'.format(bucket=bucket)]

        s3concat(*urls, remove_orig=True)

        assert _get_object_info(bucket, '1') is None
        assert _get_object_info(bucket, '2') is None
        assert _get_object_info(bucket, '3') is not None

    def test_too_few_args(self):
        with pytest.raises(ValueError):
            s3concat('s3://boo/baa')

    def test_no_objects_exist(self):
        with pytest.raises(ValueError):
            s3concat('s3://boo/baa', 's3://baa/sfeji')
