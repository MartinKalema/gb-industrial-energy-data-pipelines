#!/usr/bin/env bash
set -euo pipefail

# SimpleAuthManager is intentionally limited to this localhost-only development
# runtime. Generate a high-entropy password once, store it in the persistent
# Airflow home, and reuse it across container restarts.
airflow_auth_username="${AIRFLOW_SIMPLE_AUTH_USERNAME:-admin}"
airflow_password_file="${AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE:-${AIRFLOW_HOME:-/opt/airflow}/simple_auth_manager_passwords.json}"

if [[ ! "${airflow_auth_username}" =~ ^[A-Za-z0-9._-]+$ ]]; then
    echo "AIRFLOW_SIMPLE_AUTH_USERNAME contains unsupported characters." >&2
    exit 2
fi

export AIRFLOW_AUTH_BOOTSTRAP_USERNAME="${airflow_auth_username}"
export AIRFLOW_AUTH_BOOTSTRAP_FILE="${airflow_password_file}"
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS="${airflow_auth_username}:admin"
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE="${airflow_password_file}"

python - <<'PY'
import json
import os
from pathlib import Path
import secrets

username = os.environ["AIRFLOW_AUTH_BOOTSTRAP_USERNAME"]
password_file = Path(os.environ["AIRFLOW_AUTH_BOOTSTRAP_FILE"])
password_file.parent.mkdir(mode=0o700, parents=True, exist_ok=True)

passwords: dict[str, str] = {}
if password_file.exists() and password_file.stat().st_size:
    with password_file.open(encoding="utf-8") as stream:
        existing = json.load(stream)
    if not isinstance(existing, dict) or not all(
        isinstance(key, str) and isinstance(value, str)
        for key, value in existing.items()
    ):
        raise ValueError(f"Invalid SimpleAuth password file: {password_file}")
    passwords.update(existing)

if not passwords.get(username):
    # token_urlsafe(32) carries 256 bits of entropy and has no shell-sensitive
    # whitespace. No password or placeholder is stored in the image or Compose.
    passwords[username] = secrets.token_urlsafe(32)
    temporary_file = password_file.with_name(f".{password_file.name}.tmp")
    old_umask = os.umask(0o077)
    try:
        with temporary_file.open("w", encoding="utf-8") as stream:
            json.dump(passwords, stream, sort_keys=True)
            stream.write("\n")
        temporary_file.chmod(0o600)
        temporary_file.replace(password_file)
    finally:
        os.umask(old_umask)
    print(f"Generated a persistent SimpleAuth password for user {username!r}.")
    print("Read it with the command documented in infrastructure/airflow/README.md.")

password_file.chmod(0o600)
PY

unset AIRFLOW_AUTH_BOOTSTRAP_USERNAME AIRFLOW_AUTH_BOOTSTRAP_FILE

# Initialize/migrate the local metadata database before provisioning the
# single-writer pool. Reapplying the pool definition on startup is idempotent.
airflow db migrate
airflow pools set \
    iceberg_writer \
    1 \
    "Serialize every Trino writer to the shared Iceberg source tables"

exec airflow standalone
