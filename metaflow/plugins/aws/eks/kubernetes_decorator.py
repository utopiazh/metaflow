import os
import sys
import platform


from metaflow import R
from metaflow import util
from metaflow.decorators import StepDecorator
from metaflow.datastore.datastore import TransformableObject
from metaflow.plugins import ResourcesDecorator
from metaflow.plugins.timeout_decorator import get_run_time_limit_for_task
from metaflow.metadata import MetaDatum
from metaflow.metaflow_config import ECS_S3_ACCESS_IAM_ROLE, BATCH_JOB_QUEUE, \
                    BATCH_CONTAINER_IMAGE, BATCH_CONTAINER_REGISTRY, \
                    ECS_FARGATE_EXECUTION_ROLE, DATASTORE_LOCAL_DIR
from metaflow.sidecar import SidecarSubProcess

from .batch import Batch, BatchException
from ..aws_utils import get_docker_registry, sync_metadata_to_S3


class KubernetesDecorator(StepDecorator):
    """
    Step decorator to specify that this step should execute as a Kubernetes
    Batch job.

    Note that you can apply this decorator automatically to all steps using the
    ```--with k8s``` argument when calling run/resume. Step level decorators 
    specified in the code are overrides and will force a step to execute
    as a Kubernetes Batch job regardless of the ```--with``` specification.
    To use, annotate your step as follows:
    
    ```
    @k8s
    @step
    def myStep(self):
        ...
    ```
    Parameters
    ----------
    cpu : int
        Number of CPUs required for this step. Defaults to 1. If @resources is 
        also present, the maximum value from all decorators is used.
    gpu : int
        Number of GPUs required for this step. Defaults to 0. If @resources is 
        also present, the maximum value from all decorators is used.
    memory : int
        Memory size (in MB) required for this step. Defaults to 4096. If 
        @resources is also present, the maximum value from all decorators is 
        used.
    image : string
        Image to use when launching the Kubernetes Batch job. If not specified,
        a default image mapping to the current version of Python is used.
    """
    name = 'k8s'
    defaults = {
        'cpu': '1',
        'gpu': '0',
        'memory': '4096',
        'image': None
    }
    package_url = None
    package_sha = None
    run_time_limit = None

    def __init__(self, attributes=None, statically_defined=False):
        super(KubernetesDecorator, self).__init__(
                                            attributes, statically_defined)

        # If no docker image is explicitly specified, impute a default image.
        if not self.attributes['image']:
            if BATCH_CONTAINER_IMAGE:
                self.attributes['image'] = BATCH_CONTAINER_IMAGE
            else:
                if R.use_r():
                    self.attributes['image'] = R.container_image()
                else:
                    self.attributes['image'] = \
                        'python:%s.%s' % (platform.python_version_tuple()[0],
                                            platform.python_version_tuple()[1])
        if not get_docker_registry(self.attributes['image']):
            if BATCH_CONTAINER_REGISTRY:
                self.attributes['image'] = \
                    '%s/%s' % (BATCH_CONTAINER_REGISTRY.rstrip('/'),
                                    self.attributes['image'])

    # Refer https://github.com/Netflix/metaflow/blob/master/docs/lifecycle.png
    # to understand where these functions are invoked in the lifecycle of a
    # Metaflow flow.
    def step_init(self,
                  flow,
                  graph,
                  step,
                  decos,
                  environment,
                  datastore,
                  logger):
        # Executing Batch jobs on Kubernetes requires a non-local datastore.
        # At the moment, Metaflow's cloud-native datastore integration is with
        # Amazon S3, so we explictly check here if datastore is configured to
        # Amazon S3. As we support more clouds and local kubernetes execution,
        # we can do away with this check.
        if datastore.TYPE != 's3':
            raise KubernetesException(
                'The *@k8s* decorator requires --datastore=s3.')

        # Set internal state.
        self.logger = logger
        self.environment = environment
        self.datastore = datastore
        self.step = step
        for deco in decos:
            if isinstance(deco, ResourcesDecorator):
                for k, v in deco.attributes.items():
                    # We use the larger of @resources and @k8s attributes
                    # TODO: https://github.com/Netflix/metaflow/issues/467
                    my_val = self.attributes.get(k)
                    if not (my_val is None and v is None):
                        self.attributes[k] = \
                                        str(max(int(my_val or 0), int(v or 0)))
        
        # Set run time limit for the Kubernetes Batch job.
        self.run_time_limit = get_run_time_limit_for_task(decos)

    def runtime_init(self,
                     flow,
                     graph,
                     package,
                     run_id):
        # Set some more internal state.
        self.flow = flow
        self.graph = graph
        self.package = package
        self.run_id = run_id

    def runtime_task_created(self,
                             datastore,
                             task_id,
                             split_index,
                             input_paths,
                             is_cloned,
                             ubf_context):
        # To execute the Batch job on Kubernetes, the pod needs to have
        # access to the code package. We store the package in the datastore
        # which the pod is able to download as part of it's entrypoint. 
        if not is_cloned and self.package_url is None:
            self.package_url = datastore.save_data(
                                    self.package.sha,
                                    TransformableObject(self.package.blob))
            self.package_sha = self.package.sha

    def runtime_step_cli(self,
                         cli_args,
                         retry_count,
                         max_user_code_retries,
                         ubf_context):
        # After all attempts to run the user code have failed, we don't need
        # Kubernetes anymore. We can execute possible fallback code locally.
        if retry_count <= max_user_code_retries:
            cli_args.commands = ['k8s', 'step']
            cli_args.command_args.append(self.package_sha)
            cli_args.command_args.append(self.package_url)
            cli_args.command_options.update(self.attributes)
            cli_args.command_options['run-time-limit'] = self.run_time_limit
            if not R.use_r():
                cli_args.entrypoint[0] = sys.executable

    def task_pre_step(self,
                      step_name,
                      ds,
                      metadata,
                      run_id,
                      task_id,
                      flow,
                      graph,
                      retry_count,
                      max_retries,
                      ubf_context):
        # If `local` metadata is configured, then we would need to copy task
        # execution metadata from the Kubernetes Batch container to user's
        # local file system after the user code has finished execution. This
        # happens via datastore as a communication bridge.
        self.sync_metadata = metadata.TYPE == 'local'

        # Start MFLog sidecar to collect task logs.
        self._save_logs_sidecar = SidecarSubProcess('save_logs_periodically')      

    def task_finished(self,
                      step_name,
                      flow,
                      graph,
                      is_task_ok,
                      retry_count,
                      max_retries):
        if self.sync_metadata:
            # Note that the datastore is *always* s3 (see 
            # runtime_task_created function).
            sync_metadata_to_S3(DATASTORE_LOCAL_DIR,
                                self.datastore.root,
                                retry_count)

        try:
            self._save_logs_sidecar.kill()
        except:
            pass