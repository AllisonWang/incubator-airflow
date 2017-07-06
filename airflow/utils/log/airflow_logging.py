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
#
import logging
import os
import warnings

from airflow import configuration as conf
from airflow.utils import logging as logging_utils
from airflow.utils.file import mkdirs
from airflow.utils.log.base_airflow_logging import BaseAirflowLogging


class AirflowLogging(BaseAirflowLogging):
    """
    Default configuration for airflow logging behaviors.
    """

    def pre_task_logging(self, task_instance):
        logging.root.handlers = []
        # Setting up logging to a file.

        # To handle log writing when tasks are impersonated, the log files need to
        # be writable by the user that runs the Airflow command and the user
        # that is impersonated. This is mainly to handle corner cases with the
        # SubDagOperator. When the SubDagOperator is run, all of the operators
        # run under the impersonated user and create appropriate log files
        # as the impersonated user. However, if the user manually runs tasks
        # of the SubDagOperator through the UI, then the log files are created
        # by the user that runs the Airflow command. For example, the Airflow
        # run command may be run by the `airflow_sudoable` user, but the Airflow
        # tasks may be run by the `airflow` user. If the log files are not
        # writable by both users, then it's possible that re-running a task
        # via the UI (or vice versa) results in a permission error as the task
        # tries to write to a log file created by the other user.
        log_base = os.path.expanduser(conf.get('core', 'BASE_LOG_FOLDER'))
        directory = log_base + "/{task_instance.dag_id}/{task_instance.task_id}" \
            .format(task_instance=task_instance)
        # Create the log file and give it group writable permissions
        # TODO(aoen): Make log dirs and logs globally readable for now since the SubDag
        # operator is not compatible with impersonation (e.g. if a Celery executor is used
        # for a SubDag operator and the SubDag operator has a different owner than the
        # parent DAG)
        if not os.path.exists(directory):
            # Create the directory as globally writable using custom mkdirs
            # as os.makedirs doesn't set mode properly.
            mkdirs(directory, 0o775)
        iso = task_instance.execution_date.isoformat()
        filename = "{directory}/{iso}".format(directory=directory, iso=iso)

        if not os.path.exists(filename):
            open(filename, "a").close()
            os.chmod(filename, 0o666)

        logging_level = conf.get('core', 'LOGGING_LEVEL').upper()
        log_format = conf.get('core', 'log_format')

        logging.basicConfig(
            filename=filename,
            level=logging_level,
            format=log_format)

    def get_task_logger(self, task_instance):
        return logging.root

    def post_task_logging(self, task_instance):
        # Force the log to flush, and set the handler to go back to normal so we
        # don't continue logging to the task's log file. The flush is important
        # because we subsequently read from the log to insert into S3 or Google
        # cloud storage.
        logging.root.handlers[0].flush()
        logging.root.handlers = []

        # store logs remotely
        remote_base = conf.get('core', 'REMOTE_BASE_LOG_FOLDER')

        # deprecated as of March 2016
        if not remote_base and conf.get('core', 'S3_LOG_FOLDER'):
            warnings.warn(
                'The S3_LOG_FOLDER conf key has been replaced by '
                'REMOTE_BASE_LOG_FOLDER. Your conf still works but please '
                'update airflow.cfg to ensure future compatibility.',
                DeprecationWarning)
            remote_base = conf.get('core', 'S3_LOG_FOLDER')

        log_base = os.path.expanduser(conf.get('core', 'BASE_LOG_FOLDER'))
        relative_path = self._get_log_filename(task_instance)
        filename = os.path.join(log_base, relative_path)

        if os.path.exists(filename):
            # read log and remove old logs to get just the latest additions

            with open(filename, 'r') as logfile:
                log = logfile.read()

            remote_log_location = filename.replace(log_base, remote_base)
            # S3
            if remote_base.startswith('s3:/'):
                logging_utils.S3Log().write(log, remote_log_location)
            # GCS
            elif remote_base.startswith('gs:/'):
                logging_utils.GCSLog().write(log, remote_log_location)
            # Other
            elif remote_base and remote_base != 'None':
                logging.error(
                    'Unsupported remote log location: {}'.format(remote_base))

    def _get_log_filename(self, task_instance):
         return "{task_instance.dag_id}/{task_instance.task_id}/{iso}".format(
         task_instance=task_instance, iso=task_instance.execution_date.isoformat())
