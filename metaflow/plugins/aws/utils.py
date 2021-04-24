import os
import shlex

from metaflow.mflog import export_mflog_env_vars,\
                           capture_logs,\
                           BASH_SAVE_LOGS


class CommonTaskAttrs:
    def __init__(self,
        flow_name,
        run_id,
        step_name,
        task_id,
        attempt,
        user,
        version,
    ):
        self.flow_name = flow_name
        self.step_name = step_name
        self.run_id = run_id
        self.task_id = task_id
        self.attempt = attempt
        self.user = user
        self.version = version

    def to_dict(self, key_prefix):
        attrs = {
            key_prefix + 'flow_name': self.flow_name,
            key_prefix + 'step_name': self.step_name,
            key_prefix + 'run_id': self.run_id,
            key_prefix + 'task_id': self.task_id,
            key_prefix + 'retry_count': str(self.attempt),
            key_prefix + 'version': self.version,
        }
        if self.user is not None:
            attrs[key_prefix + 'user'] = self.user
        return attrs


def build_task_command(
    environment,
    code_package_url,
    step_name,
    step_cmds,
    flow_name,
    run_id,
    task_id,
    attempt,
    stdout_path,
    stderr_path,
):
    mflog_expr = export_mflog_env_vars(datastore_type='s3',
                                        stdout_path=stdout_path,
                                        stderr_path=stderr_path,
                                        flow_name=flow_name,
                                        step_name=step_name,
                                        run_id=run_id,
                                        task_id=task_id,
                                        retry_count=attempt,
                                        datastore_root=None)
    init_cmds = environment.get_package_commands(code_package_url)
    init_expr = ' && '.join(init_cmds)

    stdout_dir = os.path.dirname(stdout_path)
    stderr_dir = os.path.dirname(stderr_path)

    # Below we assume that stdout and stderr file are in the same (log) dir
    assert stdout_dir == stderr_dir

    # construct an entry point that
    # 1) initializes the mflog environment (mflog_expr)
    # 2) bootstraps a metaflow environment (init_expr)

    # the `true` command is to make sure that the generated command
    # plays well with docker containers which have entrypoint set as
    # eval $@
    preamble = 'true && mkdir -p %s && %s && %s' % \
                    (stdout_dir, mflog_expr, init_expr, )

    # execute step and capture logs
    step_expr = capture_logs(' && '.join(environment.bootstrap_commands(step_name) + step_cmds))

    # after the task has finished, we save its exit code (fail/success)
    # and persist the final logs. The whole entrypoint should exit
    # with the exit code (c) of the task.
    #
    # Note that if step_expr OOMs, this tail expression is never executed.
    # We lose the last logs in this scenario (although they are visible
    # still through AWS CloudWatch console).
    cmd_str = '%s && %s; c=$?; %s; exit $c' % (preamble, step_expr, BASH_SAVE_LOGS)
    return shlex.split('bash -c \"%s\"' % cmd_str)