import json
import os
import secrets
import subprocess
import tempfile
from pathlib import Path
from uuid import UUID


def main():
    binary = os.environ.get("MOKYU_TEST_BINARY", "mokyu")
    config = os.environ.get("MOKYU_TEST_CONFIG", "/config/config.toml")
    command = [binary, "--config", config, "cli", "status"]
    for args in (command, [*command, "--ready"]):
        status = json.loads(subprocess.check_output(args, timeout=6))
        assert (status["role"], status["work"], status["ready"]) == ("storage", "recovery", True)
        for field in ("media_id", "cluster_id"):
            assert UUID(status[field]).int

    with tempfile.TemporaryDirectory() as directory:
        token = Path(directory) / "token"
        token.write_text(secrets.token_hex(32), encoding="ascii")
        result = subprocess.run(
            command,
            env={**os.environ, "MOKYU_STORAGE_RECOVERY_TOKEN_FILE": str(token)},
            capture_output=True,
            timeout=6,
        )
        assert result.returncode != 0, "a different recovery credential passed the health check"
        assert b"invalid recovery credential" in result.stderr, result.stderr.decode()


if __name__ == "__main__":
    main()
