import subprocess
import time
import os
import sys

import pytest


def test_restarting_process_does_not_duplicate_ids():
    ids = set()
    env = os.environ.copy()
    env['PYTHONPATH'] = os.getcwd() + ':' + env.get('PYTHONPATH', '')
    process = subprocess.Popen(
        ["/usr/bin/env", "python", "-u", "scripts/generate.py"],
        stdout=subprocess.PIPE,
        env=env)
    time.sleep(2)
    process.kill()
    for incoming_id in process.stdout.readlines():
        incoming_id = incoming_id.strip()
        ids.add(incoming_id)

    process = subprocess.Popen(
        ["/usr/bin/env", "python", "-u", "scripts/generate.py"],
        stdout=subprocess.PIPE,
        env=env)
    time.sleep(2)
    process.kill()
    for incoming_id in process.stdout.readlines():
        incoming_id = incoming_id.strip()
        # Here's our duplicate check. Restarting the process should
        # not duplicate the ids we get from it.
        assert incoming_id not in ids
        ids.add(incoming_id)
    # And we should have got at least 2
    assert len(ids) > 1
