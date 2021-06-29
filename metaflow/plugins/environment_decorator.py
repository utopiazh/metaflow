import os

from metaflow.exception import MetaflowException
from metaflow.decorators import StepDecorator


class EnvironmentDecorator(StepDecorator):
    """
    Step decorator to assign environment variables to the user code in a step.

    The environment variables set with this decorator will be present during
    the execution of the step.

    To use, annotate your step as follows:
    ```
    @environment(vars={'MY_ENV': 'value'})
    @step
    def myStep(self):
        ...
    ```

    Parameters
    ----------
    vars : Dict
        Dictionary of environment variables to assign to your step.
    """
    name = 'environment'
    defaults = {'vars': {}}

    def runtime_step_cli(self, cli_args, retry_count, max_user_code_retries, ubf_context):
        cli_args.env.update(self.attributes['vars'].items())