from __future__ import annotations

import json
import os
from datetime import timedelta
from pathlib import Path

from ssh_operators.ssh import (
    SSHMonitorProcessOperator,
    SSHStartAndMonitorOperator,
    SSHStartProcessOperator,
)

from airflow import DAG

c = {
    "host": "0.0.0.0",
    "login": "linuxserver.io",
    "port": 2222,
    "extra": {
        "no_host_key_check": True,
        "allow_host_key_change": True,
        "key_file": Path("~/.ssh/id_rsa").expanduser().as_posix(),
    },
}


os.environ["AIRFLOW_CONN_SSH_DEFAULT"] = json.dumps(c)

with DAG(dag_id="try_async_ssh") as dag1:
    start_task = SSHStartProcessOperator(
        task_id="start",
        command="'./work_script.sh'",  # enclosed in single quotes to avoid "template" rendering
        ssh_conn_id="ssh_default",
    )
    end_task = SSHMonitorProcessOperator(
        task_id="monitor",
        config=start_task.output,
        ssh_conn_id="ssh_default",
        poke_interval=timedelta(seconds=2),
    )
with DAG(dag_id="try_async_ssh2") as dag1:
    start_task = SSHStartAndMonitorOperator(
        task_id="start",
        command="'./work_script.sh'",  # enclosed in single quotes to avoid "template" rendering
        ssh_conn_id="ssh_default",
        variable_key="my_async_ssh_task",  # this is how we store the state for this task between reschedules
    )

if __name__ == "__main__":
    dag1.test()
