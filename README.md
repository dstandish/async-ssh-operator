# Async SSH operators (Experimental)

Experimental operators for starting and polling jobs over ssh.

They are async in the sense that they submit and disconnect, and then poll for completion.

## What's in here

Three operators:

Two for the submit and monitor (sensor) style.  One where I experimented with submit and sense (in reschedule sensor style) in one operator.

## Caveats / notes

### Not deferrable, not "asyncio"

That is a much bigger effort, because we would need a new hook with a new ssh library and I did not want to take that on right now.  Just wanted to get a minimal working prototype.  The main tricky part is the server side of things and the deferrable part of it is more predictable, not really essential, and can be done as needed.

### Other notes
Right now the temporary / incidental files created on the SSH server (e.g. for logs and exit code tracking) are named with a random identifier.  Might want to change that.

There's a status file feature I partially added, which gives the process a path it could right to to update it's status (e.g. "I'm on step 5" or some indicator of progress) that could be read in each poke.  Currently I expose the env var to the process but do not read from it.

## How to try this locally

### Make an ssh server

To test it, start an SSH server:

```shell
docker run --rm \
  --name=openssh-server \
  -e PGID=1000 \
  -e PUID=1000 \
  -e TZ=Etc/UTC \
  -p 2222:2222 \
  -e PUBLIC_KEY="$(cat ~/.ssh/id_rsa.pub)" \
  -e LOG_STDOUT=true \
  linuxserver/openssh-server
```

### Define a "work" script

Next create a little script that will simulate a process that a user might want to run on a remote server.  This is what our operators will run on the SSH server.

```bash
#!/usr/bin/env bash

echo "running cmd with
LOG_FILE=$LOG_FILE
STATUS_FILE=$STATUS_FILE
PID_FILE=$PID_FILE
"
for i in $(seq 1 10);
do
    echo $i
    echo "running step $i" > $STATUS_FILE
    sleep 1
done

exit 123
```

Notice that this script exits with non-zero exit code.  This helps us test exit code handling.

### Get the work script onto server

Easiest is to bind mount the directory with `work_script.sh` into the container when you start it above.

But here are other options.

#### Use scp

First add an ssh config item for that server:
```
Host local
    User linuxserver.io
    Hostname 0.0.0.0
    Port 2222
    IdentityFile ~/.ssh/id_rsa
    PubKeyAuthentication yes
    StrictHostKeychecking no
```
Then you could use scp.

#### Use python

Another way is in python using SSH hook. At top of script define an airflow connection.

```python
import os
import json
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
```

Then upload the file:
```python
hook = SSHHook(ssh_conn_id="ssh_default")
client = hook.get_conn()
with client:
    sftp_client = client.open_sftp()
    sftp_client.put(
        Path("~/code/airflow-testing/work_script.sh").expanduser().as_posix(), remotepath="work_script.sh"
    )
```

Fix paths as appropriate.

### Run the dag

Open the example dag in this repo.  It invokes `dag.test()` at the bottom.  So you can just select all and run, etc.
