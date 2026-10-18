"""Quarantined pack manifests, cache-independent rechecks and administrator boundaries."""
import os
import uuid
import requests
from tests.storage.source_quarantine import Fixture, rejected, wait


def main():
    fixture = Fixture()
    assert fixture.backend, 'pack fixture requires MOKYU_TEST_BACKEND'
    profile = fixture.cli('storage', 'profile', 'show', fixture.name)
    profile.update(pack_enabled=True, pack_interval='24h', pack_repack_interval='24h')
    fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
    raw = os.urandom(9 * 1024 * 1024 + 17)
    fixture.put('original', raw)
    fixture.finished(fixture.cli('pack', 'run'))
    packs = fixture.api('/api/v1/storage/packs?bucket=' + fixture.bucket_id + '&state=ready')['items']
    assert packs, 'expected a real S3 pack'
    pack_id = packs[0]['id']
    detail = fixture.api('/api/v1/packs/' + pack_id)
    pack = detail['pack']
    members = detail['members']
    assert len(members) >= 2
    physical = fixture.prefix + 'packs/' + pack['location_id'].replace('-', '')[:2] + '/' + pack['location_id'].replace('-', '')
    original = fixture.bytes(physical)
    fixture.bytes(physical, bytes([original[0] ^ 1]) + original[1:])
    scope = dict(storage_id=fixture.store['id'], source='pack', id=pack_id)
    preview = fixture.preview('quarantine', scope)
    fixture.finished(fixture.execute('quarantine', preview))
    listed = fixture.api('/api/v1/maintenance/quarantines?storage_id=' + fixture.store['id'] + '&source=pack')['items']
    assert len(listed) == 1 and listed[0]['id'] == pack_id
    rejected(lambda: fixture.client.copy_object(Bucket=fixture.name, Key='copy', CopySource={'Bucket': fixture.name, 'Key': 'original'}))
    assert fixture.api('/api/v1/packs/' + pack_id)['members'] == members
    fixture.recheck(scope, state='failed')
    # A same-length backend object and a past successful read cannot clear quarantine.
    assert len(fixture.bytes(physical)) == len(original)
    fixture.bytes(physical, original)
    fixture.cli('maintenance', 'enable')
    try:
        fixture.recheck(scope)
    finally:
        fixture.cli('maintenance', 'disable')
    fixture.read('original', raw)
    assert fixture.api('/api/v1/packs/' + pack_id)['members'] == members
    assert not fixture.api('/api/v1/maintenance/quarantines?storage_id=' + fixture.store['id'] + '&source=pack')['items']
    name, password = 'source-member-' + uuid.uuid4().hex[:10], uuid.uuid4().hex
    fixture.api('/api/v1/users', dict(username=name, password=password, role='member', must_change_password=False), expected=201)
    member = requests.Session()
    login = member.post(fixture.web + '/api/v1/auth/login', headers={'Origin':fixture.web}, json=dict(username=name,password=password), timeout=30)
    assert login.status_code == 200
    headers = {'Origin':fixture.web,'X-CSRF-Token':login.json()['csrf_token']}
    for action in ('quarantine','recheck'):
        assert member.post(fixture.web + '/api/v1/maintenance/' + action + '/preview', headers=headers, json=scope, timeout=30).status_code == 403
    assert member.get(fixture.web + '/api/v1/maintenance/quarantines', params=dict(storage_id=fixture.store['id'],source='pack'), timeout=30).status_code == 403
    print('PASS exact full pack verification, immutable members, failed recheck, maintenance recovery and member denial', flush=True)


if __name__ == '__main__':
    main()