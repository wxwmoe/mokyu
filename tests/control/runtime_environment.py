"""Cached runtime observations against a real, explicitly isolated service."""
import os
from pathlib import Path

import requests

assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
url = os.environ['MOKYU_TEST_WEB']
password = Path(os.environ['MOKYU_TEST_PASSWORD_FILE']).read_text().strip()
session = requests.Session()
reply = session.post(url + '/api/v1/auth/login', headers={'Origin': url},
                     json={'username': 'tester', 'password': password}, timeout=15)
assert reply.status_code == 200, reply.status_code
status = session.get(url + '/api/v1/system/status', timeout=15).json()
assert status['deployment_mode'] == os.environ.get('MOKYU_TEST_MODE', 'single')
if status['deployment_mode'] == 'single':
    assert status['current_node'] is None
rows = session.get(url + '/api/v1/system/config', timeout=15).json()
fields = {row['key']: row for row in rows}
assert len(fields) == len(rows)
for key in ['database.program', 'database.version', 'database.schema_version',
            'runtime.version', 'runtime.os', 'runtime.arch', 'runtime.available_cpus',
            'runtime.cpu_quota', 'runtime.memory_limit', 'runtime.memory_available_at_start']:
    row = fields[key]
    assert row['origin'] == 'environment' and row['value_mode'] == 'read'
    assert row['restart'] is False
    assert bool(row['value']) != bool(row.get('value_state'))
assert fields['database.program']['value'] == os.environ['MOKYU_TEST_DATABASE_PROGRAM']
assert int(fields['database.schema_version']['value']) > 0
assert fields['runtime.os']['value'] == 'linux'
assert fields['runtime.memory_limit']['value'] == str(1024 ** 3)
assert float(fields['runtime.cpu_quota']['value']) == float(os.environ.get('MOKYU_TEST_CPU_QUOTA', '1.5'))
assert fields['instance.deployment_key']['value'] == '[redacted]'
for row in rows:
    if row['key'].endswith(('.password', '.password_file')):
        assert row['value'] == '[redacted]'
for _ in range(3):
    observed = session.get(url + '/api/v1/system/config', timeout=15).json()
    assert [row for row in observed if row['origin'] == 'environment'] == [row for row in rows if row['origin'] == 'environment']
print('PASS runtime environment, explicit mode, limits, redaction and stable observations')
