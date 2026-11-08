"""Physical carrier names used by storage fixtures."""
from uuid import UUID


def carrier_key(directory, identity, remote):
    name = UUID(str(identity)).hex
    reversed_suffix = name[::-1]
    depth = 3 if remote else 1
    shards = [reversed_suffix[i:i + 3] for i in range(0, depth * 3, 3)]
    return '/'.join([directory, *shards, name])
