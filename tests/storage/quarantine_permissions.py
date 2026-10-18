"""Revoking the initiating administrator fences a fully verified recheck."""
import uuid

import requests

from tests.storage.source_quarantine import Fixture, wait


def main():
    fixture = Fixture()
    fixture.put('original', b'recheck authorization\n' * 1024)
    physical = wait(fixture.keys, 'source was not listed')[0]
    original = fixture.bytes(physical)
    fixture.bytes(physical, b'!')
    scope = fixture.scope()
    fixture.finished(fixture.execute('quarantine', fixture.preview('quarantine', scope)))
    fixture.bytes(physical, original)
    password = uuid.uuid4().hex
    user = fixture.api('/api/v1/users', dict(username=fixture.name, password=password, role='admin',
                                           must_change_password=False), expected=201)
    actor = requests.Session()
    login = actor.post(fixture.web + '/api/v1/auth/login', headers={'Origin': fixture.web},
                       json=dict(username=fixture.name, password=password), timeout=30)
    login.raise_for_status()
    headers = {'Origin': fixture.web, 'X-CSRF-Token': login.json()['csrf_token']}
    preview = actor.post(fixture.web + '/api/v1/maintenance/recheck/preview', headers=headers, json=scope, timeout=30)
    preview.raise_for_status()
    marker = fixture.arm('source-before-commit')
    try:
        result = actor.post(fixture.web + '/api/v1/maintenance/recheck/execute', headers=headers,
                            json=dict(preview_id=preview.json()['id'], confirmation=preview.json()['confirmation']), timeout=30)
        result.raise_for_status()
        wait(lambda: marker.with_suffix('.hit').exists(), 'recheck did not reach final authorization')
        revoked = fixture.session.patch(fixture.web + '/api/v1/users/' + user['id'], headers=fixture.headers,
                                         json=dict(enabled=False), timeout=30)
        revoked.raise_for_status()
    finally:
        marker.unlink(missing_ok=True)
    fixture.finished(result.json(), 'failed')
    listed = fixture.api('/api/v1/maintenance/quarantines?storage_id=' + fixture.store['id'] + '&source=chunk')['items']
    assert len(listed) == 1 and listed[0]['revision'] == '1'
    fixture.recheck(scope)
    print('PASS revoked administrator cannot commit a verified recheck; a fresh authorized operation succeeds', flush=True)


if __name__ == '__main__':
    main()
