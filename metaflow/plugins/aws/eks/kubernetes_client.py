from collections import defaultdict

from metaflow.exception import MetaflowException

# TODO: Verify sandbox capabilities
class KubernetesClient(object):

    def __init__(self):
        # TODO: Provide different ways of configuring the kubernetes client.
        # TODO: Get rid of the kubernetes python client.
        from kubernetes import client, config
        config.load_kube_config()
        self._client = client.BatchV1Api()

    def job(self):
        return KubernetesJob(self._client)

    def attach_job(self, job_id):
        job = RunningJob(job_id, self._client)
        return job.update()



class BatchJobException(MetaflowException):
    headline = 'AWS Batch job error'


class KubernetesJob(object):

    def __init__(self, client):
        self._client = client
        tree = lambda: defaultdict(tree)
        # self.payload maps to Job v1 batch spec
        # https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.21/#jobspec-v1-batch
        self.payload = tree()

        # Instantiate job config
        self.payload['apiVersion'] = 'batch/v1'
        self.payload['kind'] = 'Job'

    # TODO: Support docker image secrets
    # TODO: Support node selection (for GPUs etc.)
    # TODO: Ascertain default terminationGracePeriodSeconds
    # TODO: Support volumes
    # TODO: Support tolerations and affinities
    # TODO: Support namespaces

    def execute(self):
        response = self._client.create_namespaced_job(body=self.payload,
                                                      namespace='default',
                                                      pretty='True')
        # TODO: Error handling here?
        print(response.status)
        print(response.to_dict())
        job = RunningJob(
                response.to_dict()['spec']['template']['metadata']['name'],
                self._client)
        return job.update()


    def name(self, name):
        self.metadata('name', name)
        template = self.payload['spec']['template']
        if 'containers' not in template['spec']:
            template['spec']['containers'] = [{}]
        template['spec']['containers'][0]['name'] = name
        return self

    def command(self, command):
        template = self.payload['spec']['template']
        if 'containers' not in template['spec']:
            template['spec']['containers'] = [{}]
        template['spec']['containers'][0]['command'] = command
        return self

    def image(self, image):
        template = self.payload['spec']['template']
        if 'containers' not in template['spec']:
            template['spec']['containers'] = [{}]
        template['spec']['containers'][0]['image'] = image
        return self

    def cpu(self, cpu):
        template = self.payload['spec']['template']
        if 'containers' not in template['spec']:
            template['spec']['containers'] = [{}]
        # TODO: Fix this!
        # template['spec']['containers'][0]['resources']['limits']['cpu'] = cpu
        # template['spec']['containers'][0]['resources']['requests']['cpu'] = cpu
        return self

    def memory(self, memory):
        template = self.payload['spec']['template']
        if 'containers' not in template['spec']:
            template['spec']['containers'] = [{}]
        # TODO: Fix this!
        # template['spec']['containers'][0]['resources']['limits']['memory'] = \
        #                                                                 memory
        # template['spec']['containers'][0]['resources']['requests']['memory'] = \
        #                                                                 memory
        return self

    def gpu(self, gpu):
        return self

    def environment_variable(self, name, value):
        template = self.payload['spec']['template']
        if 'containers' not in template['spec']:
            template['spec']['containers'] = [{}]
        if 'env' not in template['spec']['containers'][0]:
            template['spec']['containers'][0]['env'] = []
        template['spec']['containers'][0]['env'].append({'value' : str(value),
                                                         'name': name})
        return self  

    def namespace(self, namespace):
        # TODO: Handle it centrally?
        self.metadata('namespace', namespace)
        return self

    def retries(self, retries):
        # If unset, default is 6 retries
        # https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.21/#jobspec-v1-batch
        self.payload['spec']['backoffLimit'] = retries
        return self

    def timeout_in_secs(self, timeout_in_secs):
        # Set timeout at the pod level rather than at the job level. This
        # ensures that the timer is restarted when the pod restarts - mirroring
        # @timeout decorator's semantics.
        self.payload['spec']['template']['spec']['activeDeadlineSeconds'] = \
                                                                timeout_in_secs
        return self

    def scheduler(self, scheduler):
        # TODO: Test with something other than kube-scheduler
        self.payload['spec']['template']['spec']['schedulerName'] = scheduler
        return self

    def service_account(self, service_account):
        # TODO: serviceAccount is the deprecated alias for serviceAccountName.
        # Verify older versions of Kubernetes work as expected.
        self.payload['spec']['template']['spec']['serviceAccountName'] = \
                                                                service_account
        return self

    def annotation(self, key, value):
        # TODO: Do we need to set the annotations at both, job and pod level?
        self.payload['metadata']['annotations'][key] = str(value)
        self.payload['spec']['template']['metadata']['annotations'][key] = \
                                                                    str(value)
        return self

    def label(self, key, value):
        # TODO: Do we need to set the labels at both, job and pod level?
        self.payload['metadata']['labels'][key] = str(value)
        self.payload['spec']['template']['metadata']['labels'][key] = str(value)
        return self

    def metadata(self, key, value):
        # TODO: Do we need to set the metadata at both, job and pod level?
        self.payload['metadata'][key] = value
        self.payload['spec']['template']['metadata'][key] = value
        return self

    def cleanup_ttl_in_secs(self, cleanup_ttl_in_secs):
        self.payload['spec']['ttlSecondsAfterFinished'] = cleanup_ttl_in_secs
        return self

    def restart_policy(self, restart_policy):
        self.payload['spec']['template']['spec']['restartPolicy'] = \
                                                                restart_policy
        return self




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



    def kill(self):
        if not self.is_done:
            self._client.terminate_job(
                jobId=self._id, reason='Metaflow initiated job termination.')
        return self.update()


