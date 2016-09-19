# -*- coding: utf-8 -*-
#
# The MIT License (MIT)
# Copyright (c) 2016 Taro Sato
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation files
# (the "Software"), to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
# BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
from __future__ import absolute_import
import logging
from collections import defaultdict

import gevent
import gevent.pool
from botocore.exceptions import ClientError

from . import resources
from .urls import S3URL


logging.basicConfig(level='WARNING')
log = logging.getLogger(__name__)


s3 = resources.s3

KB = 1024
MB = KB**2


def _get_object_info(bucket, key):
    try:
        return s3.head_object(Bucket=bucket, Key=key)
    except ClientError:
        return None


def split(content, size):
    for i in xrange(0, len(content), size):
        yield content[i:i + size]


class _MultipartUpload(object):

    def __init__(self, bucket, key):
        self.bucket = bucket
        self.key = key
        self.upload_id = None
        self.upload_parts = []

    def __enter__(self):
        resp = s3.create_multipart_upload(Bucket=self.bucket, Key=self.key)
        self.upload_id = resp['UploadId']
        return self

    def __exit__(self, exc_t, exc_v, exc_tb):
        if exc_t:
            log.exception('Error completing multipart upload; aborting')
            s3.abort_multipart_upload(
                Bucket=self.bucket, Key=self.key, UploadId=self.upload_id)
            raise exc_v

    def start(self):
        s3.complete_multipart_upload(
            Bucket=self.bucket, Key=self.key, MultipartUpload={'Parts': [
                {'ETag': etag, 'PartNumber': i}
                for i, etag in enumerate(self.upload_parts, 1)]},
            UploadId=self.upload_id)

    def add_part(self, **kwargs):
        resp = s3.upload_part(
            Bucket=self.bucket,
            Key=self.key,
            PartNumber=len(self.upload_parts) + 1,
            UploadId=self.upload_id,
            **kwargs)
        self.upload_parts.append(resp['ETag'][1:-1])

    def add_part_copy(self, **kwargs):
        resp = s3.upload_part_copy(
            Bucket=self.bucket,
            Key=self.key,
            PartNumber=len(self.upload_parts) + 1,
            UploadId=self.upload_id,
            **kwargs)
        self.upload_parts.append(resp['CopyPartResult']['ETag'][1:-1])


def _upload_object(bucket, key, content):
    if len(content) < 5 * MB:
        s3.put_object(Bucket=bucket, Key=key, Body=content)
    else:
        with _MultipartUpload(bucket, key) as mpu:
            for part in split(content, size=5 * MB):
                mpu.add_part(Body=part)
            mpu.start()


def _concat_to_small_object(bucket, key, content):
    resp = s3.get_object(Bucket=bucket, Key=key)
    _upload_object(bucket, key, resp['Body'].read() + content)


def _concat_to_big_object(bucket, key, content):
    with _MultipartUpload(bucket, key) as mpu:
        mpu.add_part_copy(
            CopySource={'Bucket': bucket, 'Key': key})
        for part in split(content, size=5 * MB):
            mpu.add_part(Body=part)
        mpu.start()


def s3concat_content(bucket, key, content):
    info = _get_object_info(bucket, key)
    if info is None:
        _upload_object(bucket, key, content)
    else:
        if info['ContentLength'] < 5 * MB:
            _concat_to_small_object(bucket, key, content)
        else:
            _concat_to_big_object(bucket, key, content)


def s3concat(*args, **kwargs):
    if len(args) < 2:
        raise ValueError('Must specify at least two S3 objects')

    remove_orig = kwargs.get('remove_orig', False)

    primary = S3URL(args[0])

    objs = [None] * len(args)

    def get_info(idx, url):
        obj = S3URL(url)
        resp = _get_object_info(obj.bucket, obj.key)
        if resp is None:
            if idx != 0:
                log.warning('Skipping non-existing S3 object %s', obj)
            return
        objs[idx] = (obj, resp)

    pool = gevent.pool.Pool()
    for idx, url in enumerate(args):
        pool.spawn(get_info, idx, url)
    pool.join()

    objs = [o for o in objs if o is not None]
    if not objs:
        raise ValueError('None of input S3 objects exist')

    parts = []
    current_part = []
    current_part_size = 0
    for obj, info in objs:
        size = info['ContentLength']

        if current_part_size + size < 5 * MB:
            current_part.append((obj, None))
            current_part_size += size
        else:
            if current_part_size == 0:
                parts.append([(obj, (0, size - 1))])

            else:
                diff_size = 5 * MB - current_part_size
                current_part.append((obj, (0, diff_size - 1)))

                parts.append(current_part)

                if size - diff_size < 5 * MB:
                    current_part = [(obj, (diff_size, size - 1))]
                    current_part_size = size - diff_size
                else:
                    parts.append([(obj, (diff_size, size - 1))])

                    current_part = []
                    current_part_size = 0

    if current_part:
        parts.append(current_part)

    with _MultipartUpload(primary.bucket, primary.key) as mpu:
        for part_number, part in enumerate(parts, 1):
            if len(part) == 1:
                obj, byte_range = part[0]
                kwargs = {'CopySource': {'Bucket': obj.bucket, 'Key': obj.key}}
                if byte_range is not None:
                    kwargs['CopySourceRange'] = 'bytes={0}-{1}'.format(
                        *byte_range)
                mpu.add_part_copy(**kwargs)
            else:
                content = ''
                for obj, byte_range in part:
                    kwargs = {'Bucket': obj.bucket, 'Key': obj.key}
                    if byte_range is not None:
                        kwargs['Range'] = 'bytes={0}-{1}'.format(*byte_range)
                    resp = s3.get_object(**kwargs)
                    content += resp['Body'].read()
                mpu.add_part(Body=content)
        mpu.start()

    if remove_orig:
        buckets = defaultdict(set)
        for obj, _ in objs:
            if not (obj.bucket == primary.bucket and obj.key == primary.key):
                buckets[obj.bucket].add(obj.key)
        for bucket, keys in buckets.iteritems():
            keys = list(keys)
            for idx in xrange(0, len(keys), 1000):
                s3.delete_objects(
                    Bucket=bucket,
                    Delete={'Objects': [
                        {'Key': key} for key in keys[idx:idx + 1000]]})
