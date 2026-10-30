"""Bounded metadata transaction retries in isolated MySQL and MariaDB fixtures."""
import os

from tests.recovery.source_repair import Database
from tests.storage.source_quarantine import Fixture, wait


def run():
    database = Database()
    assert database.type == 'mysql'
    source, target = Fixture(), Fixture()
    for fixture in (source, target):
        profile = fixture.cli('storage', 'profile', 'show', fixture.name)
        profile.update(cdc_enabled=False, deduplication_enabled=False, compression_enabled=False,
                       pack_enabled=False, pack_range_enabled=False)
        fixture.cli('storage', 'profile', 'set', fixture.name, '-', value=profile)
    data = b'durable body across a rolled-back publication' * 100
    source.put('retry', data)

    def finish(identifier):
        return wait(lambda: (row if (row := source.api('/api/v1/replications/' + identifier))['phase'] == 'complete'
                             or row['state'] == 'failed' else None), 'transaction retry did not settle')

    def task(operation='copy'):
        return source.api('/api/v1/replications', dict(source_bucket=source.bucket_id,
            target_bucket=target.bucket_id, operation=operation,
            scope=dict(selection='objects', keys=['retry'])), expected=202)['task_id']

    assert finish(task())['state'] == 'completed'
    # A nontransactional counter survives the deliberate rollback in this disposable fixture.
    database.query('CREATE TABLE replication_retry_fixture (attempts INT, failures INT, target_state VARCHAR(16)) ENGINE=MyISAM')
    database.query("INSERT INTO replication_retry_fixture VALUES (0,2,'skipped')")
    try:
        database.query('''CREATE TRIGGER replication_retry_fixture BEFORE UPDATE ON transfer_items FOR EACH ROW
            BEGIN
                IF NEW.state=(SELECT target_state FROM replication_retry_fixture) THEN
                    UPDATE replication_retry_fixture SET attempts=attempts+1;
                    IF (SELECT attempts<=failures FROM replication_retry_fixture) THEN
                        SIGNAL SQLSTATE '40001' SET MYSQL_ERRNO=1213, MESSAGE_TEXT='isolated replication deadlock';
                    END IF;
                END IF;
            END''')
        repeated = finish(task())
        assert repeated['state'] == 'completed' and repeated['progress']['skipped'] == 1, repeated
        assert repeated['progress']['sources'] == 0
        assert database.query('SELECT attempts FROM replication_retry_fixture')[0]['attempts'] == 3
        target.read('retry', data)

        database.query("UPDATE replication_retry_fixture SET attempts=0,target_state='completed'")
        source.client.put_object_acl(Bucket=source.name, Key='retry', ACL='public-read')
        updated = finish(task())
        assert updated['state'] == 'completed' and updated['progress']['completed'] == 1, updated
        assert updated['progress']['sources'] == 0
        assert database.query('SELECT attempts FROM replication_retry_fixture')[0]['attempts'] == 3
        grants = target.client.get_object_acl(Bucket=target.name, Key='retry')['Grants']
        assert any(grant['Grantee'].get('URI') == 'http://acs.amazonaws.com/groups/global/AllUsers' for grant in grants)

        database.query('UPDATE replication_retry_fixture SET attempts=0,failures=100')
        identifier = task('move')
        failed = finish(identifier)
        assert failed['state'] == 'failed' and failed['progress']['completed'] == 0, failed
        assert database.query('SELECT attempts FROM replication_retry_fixture')[0]['attempts'] == 3
        source.read('retry', data)
        target.read('retry', data)
        database.query('DROP TRIGGER replication_retry_fixture')
        source.api('/api/v1/replications/' + identifier + '/command', 'resume', expected=202)
        assert finish(identifier)['progress']['completed'] == 1
        target.read('retry', data)
        assert not source.client.list_objects_v2(Bucket=source.name, Prefix='retry').get('Contents')
    finally:
        database.query('DROP TRIGGER IF EXISTS replication_retry_fixture')
        database.query('DROP TABLE replication_retry_fixture')
    return ['two metadata deadlocks retry without duplicate skip or ACL publication',
            'persistent deadlocks stop after three attempts, preserve Move sources and resume safely']


if __name__ == '__main__':
    assert os.environ.get('MOKYU_TEST_ALLOW_STATE_CHANGES') == 'isolated-only'
    for check in run():
        print('PASS:', check, flush=True)
