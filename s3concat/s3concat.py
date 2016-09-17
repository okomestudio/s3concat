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

import boto3
import gevent
import gevent.pool
from botocore.exceptions import ClientError

from .resources import S3URL


logging.basicConfig(level='WARNING')
log = logging.getLogger(__name__)


s3 = boto3.client('s3')
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


def _make_upload_parts(content, bucket, key, upload_id, start_from=1,
                       size=5 * MB):
    parts = []
    for part_number, part in enumerate(split(content, size), start_from):
        resp = s3.upload_part(
            Body=part,
            Bucket=bucket,
            Key=key,
            PartNumber=part_number,
            UploadId=upload_id)
        parts.append(resp['ETag'][1:-1])
    return parts


def _upload_object(bucket, key, content):
    if len(content) < 5 * MB:
        s3.put_object(Bucket=bucket, Key=key, Body=content)
    else:
        resp = s3.create_multipart_upload(Bucket=bucket, Key=key)
        upload_id = resp['UploadId']
        try:
            parts = _make_upload_parts(content, bucket, key, upload_id)
            resp = s3.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                MultipartUpload={'Parts': [
                    {'ETag': etag, 'PartNumber': i}
                    for i, etag in enumerate(parts, 1)]},
                UploadId=upload_id)
        except Exception:
            log.exception('Error completing multipart upload')
            s3.abort_multipart_upload(
                Bucket=bucket, Key=key, UploadId=upload_id)
            raise


def _concat_to_small_object(bucket, key, content):
    resp = s3.get_object(Bucket=bucket, Key=key)
    _upload_object(bucket, key, resp['Body'].read() + content)


def _concat_to_big_object(bucket, key, content):
    resp = s3.create_multipart_upload(Bucket=bucket, Key=key)
    upload_id = resp['UploadId']

    try:
        parts = []

        resp = s3.upload_part_copy(
            CopySource={'Bucket': bucket, 'Key': key},
            Bucket=bucket,
            Key=key,
            PartNumber=1,
            UploadId=upload_id)
        parts.append(resp['CopyPartResult']['ETag'][1:-1])

        parts.extend(_make_upload_parts(
            content, bucket, key, upload_id, start_from=2))

        resp = s3.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            MultipartUpload={'Parts': [
                {'ETag': etag, 'PartNumber': i}
                for i, etag in enumerate(parts, 1)]},
            UploadId=upload_id)

    except Exception:
        log.exception('Error completing multipart upload')
        s3.abort_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id)
        raise


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
    remove_orig = kwargs.get('remove_orig', False)

    if len(args) < 2:
        raise ValueError('Must specify at least two S3 objects')
    primary = S3URL(args[0])
    resp = _get_object_info(primary.bucket, primary.key)
    if resp is None:
        # primary object does not exit yet
        urls = args[1:]
    else:
        # overwrite primary object
        urls = args

    objs = {}

    def get_info(idx, url):
        obj = S3URL(url)
        resp = _get_object_info(obj.bucket, obj.key)
        if resp is None:
            log.warning('Skipping non-existing S3 object %s', obj)
            return
        objs[idx] = (obj, resp)

    pool = gevent.pool.Pool()
    for idx, url in enumerate(urls):
        pool.spawn(get_info, idx, url)
    pool.join()

    objs = [objs[i] for i in xrange(len(objs))]

    parts = []
    current_part = []
    current_part_size = 0
    for obj, info in objs:
        size = info['ContentLength']

        if current_part_size + size < 5 * MB:
            current_part.append((obj, None))
            current_part_size += size
            continue

        diff_size = 5 * MB - current_part_size
        current_part.append((obj, (0, diff_size - 1)))

        parts.append(current_part)

        if size - diff_size < 5 * MB:
            current_part = [(obj, (diff_size, size - 1))]
            current_part_size = size - diff_size
            continue

        parts.append([(obj, (diff_size, size - 1))])

        current_part = []
        current_part_size = 0

    if current_part:
        parts.append(current_part)

    resp = s3.create_multipart_upload(Bucket=primary.bucket, Key=primary.key)
    upload_id = resp['UploadId']
    try:
        upload_parts = []
        for part_number, part in enumerate(parts, 1):
            if len(part) == 1:
                obj, byte_range = part[0]
                resp = s3.upload_part_copy(
                    CopySource={'Bucket': obj.bucket, 'Key': obj.key},
                    CopySourceRange='{0}-{1}'.format(*byte_range),
                    Bucket=primary.bucket,
                    Key=primary.key,
                    PartNumber=part_number,
                    UploadId=upload_id)
                upload_parts.append(resp['CopyPartResult']['ETag'][1:-1])
            else:
                content = ''
                for obj, byte_range in part:
                    kwargs = {'Bucket': obj.bucket,
                              'Key': obj.key}
                    if byte_range is not None:
                        kwargs['Range'] = '{0}-{1}'.format(*byte_range)
                    resp = s3.get_object(**kwargs)
                    content += resp['Body'].read()
                resp = s3.upload_part(
                    Body=content,
                    Bucket=primary.bucket,
                    Key=primary.key,
                    PartNumber=part_number,
                    UploadId=upload_id)
                upload_parts.append(resp['ETag'][1:-1])

        resp = s3.complete_multipart_upload(
            Bucket=primary.bucket,
            Key=primary.key,
            MultipartUpload={'Parts': [
                {'ETag': etag, 'PartNumber': i}
                for i, etag in enumerate(upload_parts, 1)]},
            UploadId=upload_id)
    except Exception:
        log.exception('Error completing multipart upload')
        s3.abort_multipart_upload(
            Bucket=primary.bucket, Key=primary.key, UploadId=upload_id)
        raise

    log.warning('S3CONCAT FINIESHED %r', (primary))
