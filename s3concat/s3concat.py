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
from botocore.exceptions import ClientError


logging.basicConfig(level='WARNING')
log = logging.getLogger(__name__)


s3 = boto3.client('s3')
MB = 1024 ** 2


def _get_object_info(bucket, key):
    try:
        return s3.head_object(Bucket=bucket, Key=key)
    except ClientError:
        return None


def _upload_object(bucket, key, content):
    s3.put_object(Bucket=bucket, Key=key, Body=content)


def _concat_to_small_object(bucket, key, content):
    resp = s3.get_object(Bucket=bucket, Key=key)
    downloaded = resp['Body'].read()
    s3.put_object(Bucket=bucket, Key=key, Body=downloaded + content)


def _do_multipart_upload(bucket, key, content):
    origkey = key
    key = key + '.tmp'

    resp = s3.create_multipart_upload(Bucket=bucket, Key=key)
    upload_id = resp['UploadId']

    try:
        parts = []

        resp = s3.upload_part_copy(
            CopySource={'Bucket': bucket, 'Key': origkey},
            Bucket=bucket,
            Key=key,
            PartNumber=1,
            UploadId=upload_id)
        parts.append(resp['CopyPartResult']['ETag'][1:-1])
        log.warning('PARTS %r', parts)

        resp = s3.upload_part(
            Body=content,
            Bucket=bucket,
            Key=key,
            PartNumber=2,
            UploadId=upload_id)
        parts.append(resp['ETag'][1:-1])

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

    else:
        s3.delete_object(Bucket=bucket, Key=origkey)
        s3.copy_object(
            CopySource={'Bucket': bucket, 'Key': key},
            Bucket=bucket,
            Key=origkey)


def s3concat(bucket, key, content):
    info = _get_object_info(bucket, key)
    if info is None:
        _upload_object(bucket, key, content)
    else:
        if info['ContentLength'] < 5 * MB:
            _concat_to_small_object(bucket, key, content)
        else:
            _do_multipart_upload(bucket, key, content)
