# -*- coding: utf-8 -*-
from collections import defaultdict, deque
import random
import select
import sys
import time
import hashlib

try:
    unicode
except NameError:
    unicode = str
    basestring = str

from metaflow.exception import MetaflowException
from metaflow.metaflow_config import AWS_SANDBOX_ENABLED

class KubernetesClient(object):

    def __init__(self):
        from kubernetes import client, config
        config.load_kube_config()
        self._client = client.BatchV1Api()




    def job(self):
        return KubernetesBatchJob(self._client)

    def attach_job(self, job_id):
        job = RunningJob(job_id, self._client)
        return job.update()



class BatchJobException(MetaflowException):
    headline = 'AWS Batch job error'


class KubernetesBatchJob(object):

    def __init__(self, client):
        self._client = client
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def job_name(self, job_name):
        self.payload['name'] = job_name
        return self

    def image(self, image):
        self.payload['image'] = image
        return self

    def command(self, command):
        self.payload['command'] = command
        return self

    def environment_variable(self, name, value):
        if not self.payload['env'] :
            self.payload['env']= []
        self.payload['env'].append({'name': name, 'value': str(value)})
        return self

    def to_spec(self):
        spec = {
            'apiVersion': 'batch/v1', # Assuming batch/v1 for now.
            'kind': 'Job',
            'metadata': {
                'name': self.payload['name'],
                'labels': 'hello'
            },
            'spec': {
                'template': {
                    'metadata': {
                        'labels': {
                            'app': 'pi',
                        },
                    },
                    'spec': {
                        'containers': [self.payload],
                        'restartPolicy': 'Never'
                    }
                },
                'backoffLimit': 1,      # retries handled by Metaflow
                'ttlSecondsAfterFinished': 600
            }
        }
        return spec

class Throttle(object):
    def __init__(self, delta_in_secs=1, num_tries=20):
        self.delta_in_secs = delta_in_secs
        self.num_tries = num_tries
        self._now = None
        self._reset()

    def _reset(self):
        self._tries_left = self.num_tries
        self._wait = self.delta_in_secs

    def __call__(self, func):
        def wrapped(*args, **kwargs):
            now = time.time()
            if self._now is None or (now - self._now > self._wait):
                self._now = now
                try:
                    func(*args, **kwargs)
                    self._reset()
                except TriableException as ex:
                    self._tries_left -= 1
                    if self._tries_left == 0:
                        raise ex.ex
                    self._wait = (self.delta_in_secs*1.2)**(self.num_tries-self._tries_left) + \
                        random.randint(0, 3*self.delta_in_secs)
        return wrapped

class TriableException(Exception):
    def __init__(self, ex):
        self.ex = ex

class RunningJob(object):

    NUM_RETRIES = 8

    def __init__(self, id, client):
        self._id = id
        self._client = client
        self._data = {}

    def __repr__(self):
        return '{}(\'{}\')'.format(self.__class__.__name__, self._id)

    def _apply(self, data):
        self._data = data

    @Throttle()
    def _update(self):
        try:
            data = self._client.describe_jobs(jobs=[self._id])
        except self._client.exceptions.ClientError as err:
            code = err.response['ResponseMetadata']['HTTPStatusCode']
            if code == 429 or code >= 500:
                raise TriableException(err)
            raise err
        # There have been sporadic reports of empty responses to the
        # batch.describe_jobs API call, which can potentially happen if the
        # batch.submit_job API call is not strongly consistent(¯\_(ツ)_/¯).
        # We add a check here to guard against that. The `update()` call
        # will ensure that we poll `batch.describe_jobs` until we get a
        # satisfactory response at least once through out the lifecycle of
        # the job.
        if len(data['jobs']) == 1:
            self._apply(data['jobs'][0])

    def update(self):
        self._update()
        while not self._data:
            self._update()
        return self

    @property
    def id(self):
        return self._id

    @property
    def info(self):
        if not self._data:
            self.update()
        return self._data

    @property
    def job_name(self):
        return self.info['jobName']

    @property
    def job_queue(self):
        return self.info['jobQueue']

    @property
    def status(self):
        if not self.is_done:
            self.update()
        return self.info['status']

    @property
    def status_reason(self):
        return self.info.get('statusReason')

    @property
    def created_at(self):
        return self.info['createdAt']

    @property
    def stopped_at(self):
        return self.info.get('stoppedAt', 0)

    @property
    def is_done(self):
        if self.stopped_at == 0:
            self.update()
        return self.stopped_at > 0

    @property
    def is_running(self):
        return self.status == 'RUNNING'

    @property
    def is_successful(self):
        return self.status == 'SUCCEEDED'

    @property
    def is_crashed(self):
        # TODO: Check statusmessage to find if the job crashed instead of failing
        return self.status == 'FAILED'

    @property
    def reason(self):
        return self.info['container'].get('reason')

    @property
    def status_code(self):
        if not self.is_done:
            self.update()
        return self.info['container'].get('exitCode')

    def wait_for_running(self):
        if not self.is_running and not self.is_done:
            BatchWaiter(self._client).wait_for_running(self.id)

    @property
    def log_stream_name(self):
        return self.info['container'].get('logStreamName')

    def logs(self):
        def get_log_stream(job):
            log_stream_name = job.log_stream_name
            if log_stream_name:
                return BatchLogs('/aws/batch/job', log_stream_name, sleep_on_no_data=1)
            else:
                return None

        log_stream = None
        while True:
            if self.is_running or self.is_done or self.is_crashed:
                log_stream = get_log_stream(self)
                break
            elif not self.is_done:
                self.wait_for_running()

        if log_stream is None:
            return 
        exception = None
        for i in range(self.NUM_RETRIES + 1):
            try:
                check_after_done = 0
                for line in log_stream:
                    if not line:
                        if self.is_done:
                            if check_after_done > 1:
                                return
                            check_after_done += 1
                        else:
                            pass
                    else:
                        i = 0
                        yield line
                return
            except Exception as ex:
                exception = ex
                if self.is_crashed:
                    break
                #sys.stderr.write(repr(ex) + '\n')
                if i < self.NUM_RETRIES:
                    time.sleep(2 ** i + random.randint(0, 5))
        raise BatchJobException(repr(exception))

    def kill(self):
        if not self.is_done:
            self._client.terminate_job(
                jobId=self._id, reason='Metaflow initiated job termination.')
        return self.update()


