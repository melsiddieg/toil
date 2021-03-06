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
from toil.job import Job
from toil.test import ToilTest

class CachedUnpicklingJobStoreTest(ToilTest):
    """
    https://github.com/BD2KGenomics/toil/issues/817
    """
    def test(self):
        """
        Runs two identical Toil workflows with different job store paths
        """
        for _ in range(2):
            options = Job.Runner.getDefaultOptions(self._getTestJobStorePath())
            options.logLevel = "INFO"
            root = Job.wrapJobFn(parent)
            value = Job.Runner.startToil(root, options)


def parent(job):
    return job.addChildFn(child).rv()


def child():
    return 1
