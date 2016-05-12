# Copyright (C) 2015 UCSC Computational Genomics Lab
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import

import io
import traceback
import uuid
from contextlib import contextmanager
import collections
from unittest import skip

import boto
from concurrent.futures import ThreadPoolExecutor
from psutil import cpu_count
from toil.test import ToilTest, make_tests
from toil.jobStores.aws.jobStore import copyKeyMultipart


partSize = 2**20 * 5


@contextmanager
def openS3(keySize=None):
    """
    Creates an AWS bucket. If keySize is given a key of random bytes is created and its handle
    is yielded. If no keySize is given an empty bucket handle is yielded. The bucket and all
    created keys are cleaned up automatically.

    :param int keySize: Size of source key.
    """
    if keySize < 0 and keySize is not None:
        raise ValueError("Key size must be greater than zero")
    s3 = boto.connect_s3()
    bucket = s3.create_bucket('multipart-transfer-test-%s' % uuid.uuid4())
    keyName = uuid.uuid4()
    if keySize is None:
        yield bucket
    else:
        def _uploadPart(partIndex):
            if exceptions:
                return None
            else:
                try:
                    part = io.BytesIO(f.read(partSize))
                    return upload.upload_part_from_file(fp=part, part_num=partIndex+1)
                except Exception as e:
                    if partIndex <= 4:
                        exceptions[partIndex] = traceback.format_exc()
                    else:
                        exceptions[partIndex] = e.message
                    return None

        totalSize = keySize
        totalParts = (totalSize + partSize - 1) / partSize
        exceptions = collections.OrderedDict()
        upload = boto.connect_s3().get_bucket(bucket).initiate_multipart_upload(keyName)

        try:
            with open('/dev/urandom', 'rb') as f:
                with ThreadPoolExecutor(max_workers=min(cpu_count() * 16, totalParts, 128)) as executor:
                    finishedParts = set(executor.map(_uploadPart, range(0, totalParts)))

                    if exceptions:
                        raise RuntimeError()

                    assert len(finishedParts) == totalParts
            upload.complete_upload()
        except:
            upload.cancel_upload()
            raise

        yield bucket.get_key(keyName)

    for key in bucket.list():
        key.delete()

    bucket.delete()


@skip("too slow")  # Multipart copy is implicitly tested in import export tests.
class AWSMultipartCopyTest(ToilTest):
    def setUp(self):
        super(AWSMultipartCopyTest, self).setUp()

    @classmethod
    def makeTests(self):
        def _multipartCopy(self, threadPoolSize):
            with openS3(threadPoolSize * partSize) as srcKey:
                with openS3() as dstBucket:
                    copyKeyMultipart(srcKey, dstBucket, "test")
                    assert srcKey.get_contents_as_string() == dstBucket.get_key("test").get_contents_as_string()

        make_tests(_multipartCopy, targetClass=AWSMultipartCopyTest,
                   threadPoolSize={str(x): x for x in (1, 2, 16, 128)})

AWSMultipartCopyTest.makeTests()
