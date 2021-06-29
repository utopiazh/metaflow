import os
import sys
import tarfile
import time
import traceback

import click

from distutils.dir_util import copy_tree

from .batch import Batch, BatchKilledException, STDOUT_PATH, STDERR_PATH


from metaflow import util
from metaflow import R
from metaflow.datastore import MetaflowDataStore
from metaflow.datastore.local import LocalDataStore
from metaflow.datastore.util.s3util import get_s3_client
from metaflow.exception import (
    CommandException,
    METAFLOW_EXIT_DISALLOW_RETRY,
)
from metaflow.metaflow_config import DATASTORE_LOCAL_DIR
from metaflow.mflog import TASK_LOG_SOURCE
from ..aws_utils import get_docker_registry, sync_metadata_from_S3

@click.group()
def cli():
    pass

@cli.group(help='Commands related to Kubernetes on Amazon EKS.')
def k8s():
    pass

@k8s.command(
    help='Execute a single task on Kubernetes using Amazon EKS. This command '
    'calls the top-level step command inside a Kubernetes job with the given '
    'options. Typically you do not call this command directly; it is used '
    'internally by Metaflow.'
)
@click.argument('step-name')
@click.argument('code-package-sha')
@click.argument('code-package-url')
@click.option('--executable',
                help='Executable requirement for Kubernetes job.')
@click.option('--image',
                help='Docker image requirement for Kubernetes job.')
@click.option('--iam-role',
                help='IAM role requirement for Kubernetes job on Amazon EKS.')
@click.option('--cpu',
                help='CPU requirement for Kubernetes job.')
@click.option('--gpu',
                help='GPU requirement for Kubernetes job.')
@click.option('--memory',
                help='Memory requirement for Kubernetes job.')
@click.option('--run-id',
                help='Passed to the top-level 'step'.')
@click.option('--task-id',
                help='Passed to the top-level 'step'.')
@click.option('--input-paths',
                help='Passed to the top-level 'step'.')
@click.option('--split-index',
                help='Passed to the top-level 'step'.')
@click.option('--clone-path',
                help='Passed to the top-level 'step'.')
@click.option('--clone-run-id',
                help='Passed to the top-level 'step'.')
@click.option('--tag',
                multiple=True,
                default=None,
                help='Passed to the top-level 'step'.')
@click.option('--namespace',
                default=None,
                help='Passed to the top-level 'step'.')
@click.option('--retry-count',
                default=0,
                help='Passed to the top-level 'step'.')
@click.option('--max-user-code-retries',
                default=0,
                help='Passed to the top-level 'step'.')
@click.option('--run-time-limit',
                default=5 * 24 * 60 * 60, # Default is set to 5 days
                help='Run time limit in seconds for Kubernetes job.')
@click.pass_context
def step(
    ctx,
    step_name,
    code_package_sha,
    code_package_url,
    executable=None,
    image=None,
    iam_role=None,
    cpu=None,
    gpu=None,
    memory=None,
    run_time_limit=None,
    **kwargs
):
    def echo(msg, stream='stderr', batch_id=None):
        msg = util.to_unicode(msg)
        if batch_id:
            msg = '[%s] %s' % (batch_id, msg)
        ctx.obj.echo_always(msg, err=(stream == sys.stderr))

    node = ctx.obj.graph[step_name]
    if ctx.obj.datastore.datastore_root is None:
        ctx.obj.datastore.datastore_root = \
            ctx.obj.datastore.get_datastore_root_from_config(echo)

    # Set executable for job entrypoint CLI.
    if R.use_r():
        entrypoint = R.entrypoint()
    else:
        if executable is None:
            executable = ctx.obj.environment.executable(step_name)
        entrypoint = '%s -u %s' % (executable,
                                   os.path.basename(sys.argv[0]))

    # Construct job entrypoint CLI. 
    input_paths = kwargs.get('input_paths')
    split_vars = None
    if input_paths:
        max_size = 30 * 1024
        split_vars = {
            'METAFLOW_INPUT_PATHS_%d' % 
                (i // max_size): input_paths[i : i + max_size]
                    for i in range(0, len(input_paths), max_size)
        }
        kwargs['input_paths'] = ''.join('${%s}' % s for s in split_vars.keys())
    step_args = ' '.join(util.dict_to_cli_options(kwargs))
    step_cli = u'{entrypoint} {top_args} step {step} {step_args}' \
                .format(entrypoint=entrypoint,
                        top_args=' '.join(
                            util.dict_to_cli_options(ctx.parent.parent.params)),
                        step=step_name,
                        step_args=' '.join(util.dict_to_cli_options(kwargs)))
    
    # Set retry policy.
    retry_count = kwargs.get('retry_count', 0)
    retry_deco = [deco for deco in node.decorators if deco.name == 'retry']
    minutes_between_retries = None
    if retry_deco:
        minutes_between_retries = int(
            retry_deco[0].attributes.get('minutes_between_retries', 2)
        )

    # Set batch attributes
    task_spec = {
        'flow_name': ctx.obj.flow.name,
        'step_name': step_name,
        'run_id': kwargs['run_id'],
        'task_id': kwargs['task_id'],
        'retry_count': str(retry_count)
    }
    attrs = {'metaflow.%s' % k: v for k, v in task_spec.items()}
    attrs['metaflow.user'] = util.get_username()
    attrs['metaflow.version'] = ctx.obj.environment.get_environment_info()[
            'metaflow_version'
        ]

    # Set environment attributes
    env_deco = [deco for deco in node.decorators if deco.name == 'environment']
    if env_deco:
        env = env_deco[0].attributes['vars']
    else:
        env = {}

    datastore_root = os.path.join(ctx.obj.datastore.make_path(
        ctx.obj.flow.name, kwargs['run_id'], step_name, kwargs['task_id']))
    # Add the environment variables related to the input-paths argument
    if split_vars:
        env.update(split_vars)

    if retry_count:
        ctx.obj.echo_always(
            'Sleeping %d minutes before the next retry' % minutes_between_retries
        )
        time.sleep(minutes_between_retries * 60)

    # this information is needed for log tailing
    spec = task_spec.copy()
    spec['attempt'] = int(spec.pop('retry_count'))
    ds = ctx.obj.datastore(mode='w', **spec)
    stdout_location = ds.get_log_location(TASK_LOG_SOURCE, 'stdout')
    stderr_location = ds.get_log_location(TASK_LOG_SOURCE, 'stderr')

    batch = Batch(ctx.obj.metadata, ctx.obj.environment)
    try:
        with ctx.obj.monitor.measure('metaflow.batch.launch'):
            batch.launch_job(
                step_name,
                step_cli,
                task_spec,
                code_package_sha,
                code_package_url,
                ctx.obj.datastore.TYPE,
                image=image,
                iam_role=iam_role,
                cpu=cpu,
                gpu=gpu,
                memory=memory,
                run_time_limit=run_time_limit,
                env=env,
                attrs=attrs
            )
    except Exception as e:
        print(e)
        sync_metadata_from_S3(ctx.obj.metadata, datastore_root, retry_count)
        sys.exit(METAFLOW_EXIT_DISALLOW_RETRY)
    try:
        batch.wait(stdout_location, stderr_location, echo=echo)
    except BatchKilledException:
        # don't retry killed tasks
        traceback.print_exc()
        sync_metadata_from_S3(ctx.obj.metadata, datastore_root, retry_count)
        sys.exit(METAFLOW_EXIT_DISALLOW_RETRY)
    sync_metadata_from_S3(ctx.obj.metadata, datastore_root, retry_count)
