# gmail-drive-wal-archiver — Run notes (fail-closed)

`requirements.txt` already PRESENT at repo root:

- google-api-python-client
- google-auth
- google-auth-oauthlib
- google-auth-httplib2

## Path A — Local unit tests (safe; no mail; no secrets)

Uses mocks only. Does **not** send mail. Does **not** require OAuth tokens.

```bash
python -m pip install -r requirements.txt
python -m compileall gmail_wal_archiver.py test_gmail_wal_archiver.py
python -m unittest test_gmail_wal_archiver.py -v
```

## Path B — Live Gmail / Drive / Sheets (BLOCKED until OAuth)

Live validation is pending per README and `KNOWN_LIMITATIONS.md`.

Blockers:

1. Google API OAuth credentials required (store out of repo; honor `.gitignore`).
2. First live run must use a **bounded date window** and manual inspection.
3. Do **not** send mail from this tool path as part of archive recovery docs here.
4. Do **not** commit or paste client secrets, tokens, or credential JSON into the repo.
5. Sheets replay may duplicate rows if append succeeds but local synced-state rewrite fails (see KNOWN_LIMITATIONS.md).

Status: candidate build — local mocks yes; live APIs unverified.
