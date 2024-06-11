from __future__ import annotations

import json
import secrets
import string
from datetime import timedelta
from typing import Any

from airflow.exceptions import AirflowException, AirflowRescheduleException
from airflow.models import BaseOperator, TaskReschedule, Variable
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.sensors.base import BaseSensorOperator, PokeReturnValue
from airflow.ti_deps.deps.ready_to_reschedule import ReadyToRescheduleDep
from airflow.utils import timezone
from airflow.utils.context import Context
from airflow.utils.session import create_session


def rand_str(num):
    """Generate random lowercase alphanumeric string of length num.

    :meta private:
    """
    alphanum_lower = string.ascii_lowercase + string.digits
    return "".join(secrets.choice(alphanum_lower) for _ in range(num))


def start_process(*, ssh_conn_id, remote_host, environment, logger, command):
    # we run the bash command in a subshell so that we can capture the exit code
    identifier = rand_str(8)
    log_file = f"{identifier}.log"
    pid_file = f"{identifier}.pid"
    status_file = f"{identifier}.status"  # not used right now
    exit_code_file = f"{identifier}.exit_code"

    # the process invoked here (self.command) could optionally write messages
    # to STATUS_FILE if it wanted e.g. to say what step it's on
    # and we could look at this e.g. when poking
    # but it's not used right now -- just an idea
    cmd = f"""
    nohup bash -c '
        (
          set -e
          STATUS_FILE={status_file} LOG_FILE={log_file} bash {command}
        )
        echo -n $? > "{exit_code_file}"
    ' >{log_file} 2>&1 &
    echo -n $! >{pid_file}
    cat {pid_file}
    """
    hook = SSHHook(
        ssh_conn_id=ssh_conn_id,
        remote_host=remote_host,
    )
    client = hook.get_conn()
    with client:
        r = hook.exec_ssh_client_command(
            ssh_client=client,
            command=cmd,
            get_pty=False,
            environment=environment,
        )
    pid = int(r[1].splitlines()[-1])
    config = {
        "log_file": log_file,
        "pid_file": pid_file,
        "status_file": status_file,
        "exit_code_file": exit_code_file,
        "pid": pid,
    }
    logger.info("started command: %s", json.dumps(config, indent=2))
    return config


def process_is_running(*, hook, client, pid, environment=None):
    exit_code, stdin, stderr = hook.exec_ssh_client_command(
        ssh_client=client,
        command=f"ps -p {pid} > /dev/null",
        get_pty=False,
        environment=environment,
    )
    return exit_code == 0


def fetch_logs(hook, client, log_file, from_line=0, environment=None):
    """
    Log all messages from the remote command.

    The hook already logs messages from stdin and stdout, so we don't need to.

    We could use from_line to log from certain line, if we update the code
    to log incrementally on each poke.
    """
    hook.exec_ssh_client_command(
        ssh_client=client,
        command=f"tail +{from_line} {log_file}",
        get_pty=False,
        environment=environment,
    )


def get_exit_code(hook, client, exit_code_file, environment=None):
    exit_code, stdin, stderr = hook.exec_ssh_client_command(
        ssh_client=client,
        command=f"cat {exit_code_file}",
        get_pty=False,
        environment=environment,
    )
    return int(stdin.strip())


def poke(config, ssh_conn_id, remote_host, environment=None):
    pid = config["pid"]
    log_file = config["log_file"]
    exit_code_file = config["exit_code_file"]
    hook = SSHHook(
        ssh_conn_id=ssh_conn_id,
        remote_host=remote_host,
    )
    client = hook.get_conn()
    with client:
        is_running = process_is_running(hook=hook, client=client, pid=pid, environment=environment)
        if is_running:
            return False
        # process is done
        fetch_logs(hook=hook, client=client, log_file=log_file, from_line=0)
        exit_code = get_exit_code(hook=hook, client=client, exit_code_file=exit_code_file)
        exit_message = f"The remote process exited with code {exit_code}"
        if exit_code != 0:
            raise AirflowException(exit_message)
        return True


class SSHStartProcessOperator(SSHOperator):
    """
    Start SSH job and disconnect.

    Must be followed by sensor SSHMonitorProcessOperator.
    """

    def execute(self, context: Context = None) -> Any:
        config = start_process(
            ssh_conn_id=self.ssh_conn_id,
            remote_host=self.remote_host,
            environment=self.environment,
            logger=self.log,
            command=self.command,
        )
        return config


class SSHMonitorProcessOperator(BaseSensorOperator):
    """Monitor ssh job created by SSHStartProcessOperator."""

    template_fields = ("config",)

    def __init__(
        self,
        config: dict,
        ssh_conn_id: str,
        remote_host: str | None = None,
        environment: dict | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.config = config
        self.ssh_conn_id = ssh_conn_id
        self.remote_host = remote_host
        self.environment = environment
        self.mode = "reschedule"

    def poke(self, context: Context) -> bool | PokeReturnValue:
        return poke(
            config=self.config,
            ssh_conn_id=self.ssh_conn_id,
            remote_host=self.remote_host,
            environment=self.environment,
        )


def has_reschedule(ti):
    with create_session() as session:
        tr = session.scalar(
            TaskReschedule.stmt_for_task_instance(ti=ti)
            .with_only_columns(TaskReschedule.reschedule_date)
            .limit(1)
        )
    return tr is not None


class SSHStartAndMonitorOperator(SSHOperator):
    """
    This operator will submit an SSH command and poll for completion.

    It uses "reschedule sensor"-like behavior and the poll interval is hardcoded
    at 30 seconds.
    """

    deps = BaseOperator.deps | {ReadyToRescheduleDep()}

    def __init__(self, variable_key, **kwargs):
        super().__init__(**kwargs)
        self.variable_key = variable_key
        self.reschedule = True

    def execute(self, context: Context = None) -> Any:
        if not has_reschedule(ti=context["ti"]):
            config = start_process(
                ssh_conn_id=self.ssh_conn_id,
                remote_host=self.remote_host,
                environment=self.environment,
                logger=self.log,
                command=self.command,
            )
            Variable.set(key=self.variable_key, value=config, serialize_json=True)
        else:
            config = Variable.get(self.variable_key, deserialize_json=True)
        result = poke(
            config=config,
            ssh_conn_id=self.ssh_conn_id,
            remote_host=self.remote_host,
            environment=self.environment,
        )
        if not result:
            raise AirflowRescheduleException(reschedule_date=timezone.utcnow() + timedelta(seconds=30))
