# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


class BaseAirflowLogging(object):
    """
    A base class for setting up custom airflow logging behaviors.
    To define your custom airflow logging config, add a `airflow_logging.py`
    file to your `AIRFLOW_HOME` and implement this BaseAirflowLogging.
    """

    def pre_task_logging(self, task_instance):
        """
        Initialize logging before task instance gets executed by worker.
        :arg task_instance: Task instance object
        """
        raise NotImplementedError()

    def get_task_logger(self, task_instance):
        """
        Return logger for given task instance.
        :arg task_instance: Task instance object
        """
        raise NotImplementedError()

    def post_task_logging(self, task_instance):
        """
        Perform any logging operations once task instance finishes executing.
        :arg task_instance: Task instance object
        """
        raise NotImplementedError()
